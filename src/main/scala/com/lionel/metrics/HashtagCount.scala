package com.lionel.metrics

import java.io.File

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import com.github.tototoshi.csv.CSVReader
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.convert.wrapAsScala._
import scala.util.control.Breaks.{break, breakable}

case class HashtagResult(nameID:String, timestamp:Long, tag:String, trackterm:String, count:Int ) extends JsonResult[HashtagResult] {
  override def +(other:HashtagResult):HashtagResult = this.copy(count = count + other.count)
  override def toJson():String = {
    s"""{
       |    "cnt": ${count},
       |    "${nameID}": ${JsonResult.cleanAndQuote(tag)},
       |    "timestamp": ${timestamp},
       |    "trackterm": ${JsonResult.cleanAndQuote(trackterm)}
       |}
      """.stripMargin
  }

  override val key = tag + trackterm
  override def total() = count
}

object HashtagCount {

  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[HashtagResult], props:TweetProps):Unit = {
    stream
      .filter(_.hasHashtags)
      .flatMap(t => t.hashtags.map(hashtag => HashtagResult("hashtag", t.date, "#"+hashtag.toLowerCase, t.searchTerm, 1)))
      .keyBy(h => h.key.toLowerCase)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .filter(_.count > 1)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Hashtag Counter ###")
  }

  def addBatchMetric(batch: DataSet[Tweet], outputFile:String):Unit = {
    batch
      .filter(_.hasHashtags)
      .flatMap(t => t.hashtags.map(hashtag => HashtagResult("hashtag", t.date, "#"+hashtag.toLowerCase, t.searchTerm, 1)))
      .groupBy(h => h.key.toLowerCase)
      .reduce(_ + _)
      .filter(_.count > 1)
      .map(t=>(t.timestamp,t.tag,t.trackterm,t.count))
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Hashtag Count")
  }

  def addBatchMetricTopN(batch: DataSet[Tweet], n:Int, outputFile:String):Unit = {
    batch
      .filter(_.hasHashtags)
      .flatMap(t => t.hashtags.map(hashtag => HashtagResult("hashtag", t.date, "#"+hashtag.toLowerCase, t.searchTerm, 1)))
      .map(t=>(t.timestamp,t.tag,t.trackterm,t.count))
      .groupBy(1, 2)
      .sum(3)
      .groupBy(2)
      .sortGroup(3, Order.DESCENDING)
      .first(n)
      .map(t=>(t._2,t._3)) // tag | trackterm
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Hashtag Count")
  }

  def compareMetrictoBatch(stream: DataStream[Tweet], hashtagN:DataStream[(String, String)], sink: SinkFunction[HashtagResult], props:TweetProps):Unit = {
    stream
      .filter(_.hasHashtags)
      .flatMap(t => t.hashtags.map(hashtag => HashtagResult("hashtag", t.date, "#"+hashtag.toLowerCase, t.searchTerm, 1)))
      .keyBy(h => h.key.toLowerCase)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .connect(hashtagN)
      .keyBy(_.trackterm,_._2)
      .flatMap(new HashtagFilter)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Hashtag Compare ###")
  }
}

class HashtagFilter extends RichCoFlatMapFunction[HashtagResult,(String,String),HashtagResult]{ // already keyed
  private var compare: ListState[(String,String)] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    compare = getRuntimeContext.getListState(new ListStateDescriptor[(String,String)]("hashtagfilter", createTypeInformation[(String,String)]))
  }

  override def flatMap1(t: HashtagResult, out: Collector[HashtagResult]): Unit = {
    breakable {
      for (elem:(String,String) <- compare.get) {
        if (elem._2 == t.trackterm && elem._1 == t.tag) {
          out.collect(HashtagResult(t.nameID, t.timestamp, t.tag, t.trackterm, t.count))
          break
        }
      }
    }
  }

  override def flatMap2(elem: (String,String), out: Collector[HashtagResult]): Unit = { //load into listState
    compare.add(elem)
    println("pushed into state " + elem)
  }
}

//
//class ContainsFilterFunction extends RichFilterFunction[HashtagResult] {
//  private var compare: ListState[(String, String)] = _
//
//  override def open(parameters: Configuration): Unit = {
//    compare = getRuntimeContext.getListState(
//      new ListStateDescriptor[(String, String)]("hashtagTopN", createTypeInformation[(String,String)])
//    )
//  }
//
//  override def filter(value: HashtagResult): Boolean = {
//    // load file on first try
//    if(compare.get().toList.length == 0){
//      // load csv file into ListState splitted by trackterm
//      val load = CSVReader.open(new File("batch/hashtagTopN.csv")).all().map(e=>(e.head.split(";")(0),e.head.split(";")(1))).filter(_._2 == value.trackterm).asJava
//      compare.update(load)
//    }
//    compare.get().toList.contains((value.tag,value.trackterm))
//  }
//}
