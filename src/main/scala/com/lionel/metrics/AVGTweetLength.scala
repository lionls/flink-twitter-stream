package com.lionel.metrics

import java.sql.{Time, Timestamp}
import java.util
import java.util.Calendar

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration

import scala.collection.convert.wrapAll._
import scala.collection.mutable

/**
  * AVG Likes per Original Tweet per Time Window
  */

case class AVGTweetLengthResult(timestamp:Long, length:Int, trackterm:String, count:Int ) extends JsonResult[AVGTweetLengthResult] {
  override def +(other:AVGTweetLengthResult):AVGTweetLengthResult = this.copy(count = count + other.count, length = length + other.length, trackterm = other.trackterm)
  override def toJson():String = {
    s"""{
       |    "cnt": ${count},
       |    "timestamp": ${timestamp},
       |    "length": ${length},
       |    "trackterm": ${JsonResult.cleanAndQuote(trackterm)}
       |}
      """.stripMargin
  }

  override val key = trackterm
  override def total() = count
}

object AVGTweetLength {

  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[AVGTweetLengthResult], props:TweetProps):Unit = {
    stream
      .filter(!_.text.isEmpty)
      .filter(_.tweetType == "Original Tweet")
      .map(t => AVGTweetLengthResult(t.date,t.text.length,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGTweetLength())
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Tweet Length")
  }

  def addBatchMetric(batch: DataSet[Tweet], outputFile:String):Unit = {
    batch
      .filter(!_.text.isEmpty)
      .filter(_.tweetType == "Original Tweet")
      .map(t => AVGTweetLengthResult(t.date,t.text.length,t.searchTerm,1))
      .groupBy(_.key)
      .reduce(_+_)
      .map(t => (t.timestamp,(t.length/t.count).toInt,t.trackterm,1))
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("AVG Tweet lenght")
  }

  def compareMetrictoBatch(stream: DataStream[Tweet], compare:List[(String,String)], sink: SinkFunction[AVGTweetLengthResult], props:TweetProps):Unit = {
    stream
      .filter(!_.text.isEmpty)
      .filter(_.tweetType == "Original Tweet")
      .map(t => AVGTweetLengthResult(t.date,t.text.length,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGTweetLength())
      .map(t=> {
        val x:(String,String) = compare.filter(x=> x._2.equals(t.trackterm))(0)
        AVGTweetLengthResult(t.timestamp,t.length-x._1.toInt,t.trackterm,1)
      })
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Tweet length")
  }

  def addPrediction(stream: DataStream[Tweet], compare:List[(String,String)], sink: SinkFunction[AVGTweetLengthResult], props:TweetProps):Unit = {
    stream
      .filter(!_.text.isEmpty)
      .filter(_.tweetType == "Original Tweet")
      .map(t => AVGTweetLengthResult(t.date,t.text.length,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGTweetLength())
      .map(new AVGTweetLengthPred(compare))
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Tweet length")
  }

}


class AVGTweetLengthPred(compare:List[(String,String)]) extends RichMapFunction[AVGTweetLengthResult, AVGTweetLengthResult] { // predict mean between new and old val
  var preds = new mutable.HashMap[String,Int]

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 100
    }
  }

  // get old data
  override def open(config:Configuration ): Unit = {
    compare.map(i => preds.put(i._2, toInt(i._1)))
  }

  @throws[Exception]
  override def map(curr: AVGTweetLengthResult): AVGTweetLengthResult = {
    var prePred:Int = 0

    for (key <- preds.keySet) {
      if (key == curr.trackterm) {
        prePred = preds.getOrDefault(key,100)
      }
    }
    // Mean
    val newPred:Int = ((prePred + curr.length) / 2).toInt
    preds.put(curr.trackterm, newPred)
    AVGTweetLengthResult(curr.timestamp,newPred,curr.trackterm+"#prediction",curr.count)
  }
}

class AVGTweetLength extends ProcessWindowFunction[AVGTweetLengthResult, AVGTweetLengthResult, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[AVGTweetLengthResult], out: Collector[AVGTweetLengthResult]): Unit = {
    val time = context.currentProcessingTime
    val avgres = elements.foldLeft(AVGTweetLengthResult(time,0,"term",0))(_+_)
    val (sum,cnt) = (avgres.length,avgres.count)

    out.collect(AVGTweetLengthResult(avgres.timestamp,sum/cnt,avgres.trackterm,1))
  }
}
