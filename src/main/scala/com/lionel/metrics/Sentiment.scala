package com.lionel.metrics

import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.streaming.api.scala._
import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.collection.convert.wrapAll._

case class SentimentResult(nameID:String, timestamp:Long, tweetType:String, sentiment:String, trackterm: String, count:Int ) extends JsonResult[SentimentResult] {
  override def +(other:SentimentResult):SentimentResult = this.copy(count = count + other.count)
  override def toJson():String = {
    s"""{
       |    "cnt": ${count},
       |    "${nameID}": ${JsonResult.cleanAndQuote(tweetType)},
       |    "timestamp": ${timestamp},
       |    "sentiment": ${JsonResult.cleanAndQuote(sentiment)},
       |    "trackterm": ${JsonResult.cleanAndQuote(trackterm)}
       |}
      """.stripMargin
  }

  override val key = sentiment + trackterm
  override def total() = count
}

object Sentiment {

  private val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  private val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  def getSentiment(input: String): String = {
    if (Option(input).isDefined && !input.trim.isEmpty) {
      val annotation: Annotation = pipeline.process(input)
      val (_, sentiment) =
        annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
          .map { sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])) }
          .map { case (sentence, tree) => (sentence.toString, getSentimentType(RNNCoreAnnotations.getPredictedClass(tree))) }
          .maxBy { case (sentence, _) => sentence.length }
      sentiment
    } else {
      throw new IllegalArgumentException("ERROR running Sentiment")
    }
  }


  private def getSentimentType(sentiment: Int): String = sentiment match {
    case x if x == 3 || x == 4 => "POSITIVE"
    case x if x == 0 || x == 1 => "NEGATIVE"
    case 2 => "NEUTRAL"
  }


  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[SentimentResult], props:TweetProps):Unit = {
    stream
      .filter(t => t.text.isEmpty == false && t.tweetType == "Original Tweet") //just analyze tweets otherwise its to resource intensive
      .map(new SentimentKit)
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_+_)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Tweet Semantics")
  }

  def addBatchMetric(batch: DataSet[Tweet], outputFile:String):Unit = {
    batch
      .filter(t => t.text.isEmpty == false && t.tweetType == "Original Tweet") //just analyze tweets otherwise its to resource intensive
      .map(new SentimentKit)
      .groupBy(_.key)
      .reduce(_+_)
      .map(t=>(t.timestamp,t.sentiment,t.trackterm,t.count))
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Sentiment")
  }

}

private class SentimentKit extends MapFunction[Tweet, SentimentResult] {

  override def map(value: Tweet):SentimentResult = {


    try{
      val sentiment = Sentiment.getSentiment(value.text.replaceAll("[^\\p{L}\\p{N}\\p{Z}\\p{Sm}\\p{Sc}\\p{Sk}\\p{Pi}\\p{Pf}\\p{Pc}\\p{Mc}\\p{Cf}\\p{Cs}]",""))
      return SentimentResult("tweetType",value.date,value.tweetType,sentiment,value.searchTerm,1)
    }catch{
      case _: Throwable => SentimentResult("tweetType",value.date,value.tweetType,"ERROR",value.searchTerm,1)
    }

    //println(SentimentResult("tweetType",value.date,value.tweetType,sentiment,1))
  }
}
