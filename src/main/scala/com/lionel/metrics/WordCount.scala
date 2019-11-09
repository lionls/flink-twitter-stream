package com.lionel.metrics

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.util.Collector
import java.util
import org.apache.flink.api.common.operators.Order

import org.apache.flink.streaming.api.datastream.KeyedStream

import scala.collection.JavaConverters._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.core.fs.FileSystem

case class WordResult( timestamp:Long, trackterm:String, word:String, count:Int ) extends JsonResult[WordResult] {
  override def +(other:WordResult):WordResult = this.copy(count = count + other.count)
  override def toJson():String = {
    s"""{
       |    "cnt": ${count},
       |    "timestamp": ${timestamp},
       |    "word": ${JsonResult.cleanAndQuote(word)},
       |    "trackterm": ${JsonResult.cleanAndQuote(trackterm)}
       |}
      """.stripMargin
  }

  override val key = word + trackterm
  override def total() = count
}

object WordCount {
  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[WordResult], props:TweetProps):Unit = {
    stream
      .flatMap(new Tokenizer)
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .filter(_.count > 1)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Word Count")
  }

  def addBatchMetric(batch: DataSet[Tweet], outputFile:String):Unit = {
    batch
      .flatMap(new Tokenizer)
      .groupBy(_.key)
      .reduce(_ + _)
      .filter(_.count > 10) // filter irrelevant words
      .map(t=>(t.timestamp,t.word,t.trackterm,t.count))
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Word Count")
  }

  def addBatchMetricTopN(batch: DataSet[Tweet], n:Int, outputFile:String):Unit = {
    batch
      .flatMap(new Tokenizer)
      .map(t=>(t.timestamp,t.word,t.trackterm,t.count))
      .groupBy(1, 2)
      .sum(3)
      .groupBy(2)
      .sortGroup(3, Order.DESCENDING)
      .first(n)
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Word Count")
  }

  def compareMetrictoBatch(stream: DataStream[Tweet], compare:List[(String,String)],sink: SinkFunction[WordResult], props:TweetProps):Unit = {
    stream
      .flatMap(new Tokenizer)
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .filter(t => compare.contains((t.trackterm,t.word)))
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Word Count")
  }
}



class Tokenizer extends FlatMapFunction[Tweet,WordResult] {
  val stopWords = List("a","be","to","i","am","of","it","t","s","and","a","in","the","is","that","an","on","https","co","this","","so","have","they","what","at","by","m","are","has","me","re","he","for","you","with")
  override def flatMap(t:Tweet, out: Collector[WordResult]): Unit = { // normalize and split the line
    val tokens = t.text.toLowerCase.split("\\W+")
    // emit the pairs
    for (token <- tokens) {
      if (token.length > 0 && !stopWords.contains(token)) out.collect(WordResult(t.date,t.searchTerm,token.toLowerCase(),1))
    }
  }
}

