package com.lionel.metrics



import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.operators.ScalaCsvOutputFormat
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

import org.apache.flink.core.fs.Path

/**
  * Counts tweets per Timewindow
  */

case class TotalCountResult(nameID:String, timestamp:Long, tweetType:String, trackterm:String, count:Int ) extends JsonResult[TotalCountResult] {
  override def +(other:TotalCountResult):TotalCountResult = this.copy(count = count+other.count)
  override def toJson():String = {
    s"""{
       |    "cnt": ${count},
       |    "${nameID}": ${JsonResult.cleanAndQuote(tweetType)},
       |    "timestamp": ${timestamp},
       |    "trackterm": ${JsonResult.cleanAndQuote(trackterm)}
       |}
      """.stripMargin
  }

  override val key = tweetType + trackterm
  override def total() = count
}

object TotalCount {

  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[TotalCountResult], props:TweetProps):Unit = {
      stream
        .map(t => TotalCountResult("tweetType",t.date,t.tweetType,t.searchTerm,1))
        .keyBy(_.key)
        .timeWindow(props.windowTime)
        .reduce(_ + _)
        .addSink(sink)
        .setParallelism(props.parallelism)
        .name("Tweet Count")
  }

  def addBatchMetric(batch: DataSet[Tweet], outputFile:String):Unit = {
    batch
      .map(t => TotalCountResult("tweetType",t.date,t.tweetType,t.searchTerm,1))
      .groupBy(_.key)
      .reduce(_ + _)
      .map(t=>(t.timestamp,t.trackterm,t.tweetType,t.count))
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Tweet Count")
  }

  def compareMetrictoBatch(stream: DataStream[Tweet], compare:List[(String,String,Long)], sink: SinkFunction[TotalCountResult], props:TweetProps):Unit = {
    stream
      .map(t => TotalCountResult("tweetType",t.date,t.tweetType,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .map(t=> {
        val x:(String,String,Long) = compare.filter(x=> (x._1.equals(t.trackterm) && x._2.equals(t.tweetType)))(0)
        TotalCountResult("tweetType", t.timestamp, t.tweetType, t.trackterm, (t.count-x._3).toInt)
      })
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Total Count")
  }

}

