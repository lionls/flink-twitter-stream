package com.lionel.metrics

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.util.Collector
import java.util

import org.apache.flink.api.common.operators.Order
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction

import scala.collection.JavaConversions._
import scala.util.control.Breaks.{break, breakable}

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
      .map(t=>(t._2,t._3)) // word | trackterm
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Word Count")
  }

  def compareMetrictoBatch(stream: DataStream[Tweet], wordcount:DataStream[(String,String)],sink: SinkFunction[WordResult], props:TweetProps):Unit = {
    stream
      .flatMap(new Tokenizer)
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .connect(wordcount)
      .keyBy(_.trackterm,_._2)
      .flatMap(new WordCountFilter)
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

class WordCountFilter extends RichCoFlatMapFunction[WordResult,(String,String),WordResult]{ // already keyed
  private var compare: ListState[(String,String)] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    compare = getRuntimeContext.getListState(new ListStateDescriptor[(String,String)]("wordfilter", createTypeInformation[(String,String)]))
  }

  override def flatMap1(t: WordResult, out: Collector[WordResult]): Unit = {
    breakable {
      for (elem:(String,String) <- compare.get) {
        if (elem._2 == t.trackterm && elem._1 == t.word) {
          out.collect(WordResult(t.timestamp,t.trackterm,t.word,t.count))
          break
        }
      }
    }
  }

  override def flatMap2(elem: (String,String), out: Collector[WordResult]): Unit = { //load into listState
    compare.add(elem)
    println("pushed into state " + elem)
  }
}