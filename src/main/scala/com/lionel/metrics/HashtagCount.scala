package com.lionel.metrics

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

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
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Hashtag Count")
  }

  def compareMetrictoBatch(stream: DataStream[Tweet], compare:List[(String,String)], sink: SinkFunction[HashtagResult], props:TweetProps):Unit = {
    stream
      .filter(_.hasHashtags)
      .flatMap(t => t.hashtags.map(hashtag => HashtagResult("hashtag", t.date, "#"+hashtag.toLowerCase, t.searchTerm, 1)))
      .keyBy(h => h.key.toLowerCase)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .filter(t => compare.contains((t.trackterm,t.tag)))
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Hashtag Compare ###")
  }
}
