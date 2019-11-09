package com.lionel.metrics

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

/**
  * Counts tweets per Timewindow
  */

case class CountResult(nameID:String, timestamp:Long, tweetType:String, count:Int ) extends JsonResult[CountResult] {
  override def +(other:CountResult):CountResult = this.copy(count = count+other.count)
  override def toJson():String = {
    s"""{
       |    "cnt": ${count},
       |    "${nameID}": ${JsonResult.cleanAndQuote(tweetType)},
       |    "timestamp": ${timestamp}
       |}
      """.stripMargin
  }

  override val key = tweetType
  override def total() = count
}

object CountTrackTerms {

  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[CountResult], props:TweetProps):Unit = {
      stream
        .map(t => CountResult("trackterm",t.date,t.searchTerm,1))
        .keyBy(_.key)
        .timeWindow(props.windowTime)
        .reduce(_ + _)
        .addSink(sink)
        .setParallelism(props.parallelism)
        .name("Trackterm Count")
  }

  def addBatchMetric(batch: DataSet[Tweet], outputFile:String):Unit = {
    batch
      .map(t => CountResult("trackterm",t.date,t.searchTerm,1))
      .groupBy(_.key)
      .reduce(_ + _)
      .map(t=>(t.timestamp,t.tweetType,t.count))
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Trackterm Count")

  }
}
