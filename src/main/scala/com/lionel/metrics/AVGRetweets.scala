package com.lionel.metrics

import java.sql.{Time, Timestamp}
import java.util.Calendar

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.convert.wrapAll._
import scala.collection.mutable

/**
  * AVG Retweets per Original Tweet per Time Window
  */

case class AVGRetweetsResult(nameID:String, timestamp:Long, tweetType:String, retweets:Double, trackterm:String, count:Int ) extends JsonResult[AVGRetweetsResult] {
  override def +(other:AVGRetweetsResult):AVGRetweetsResult = this.copy(count = count + other.count, retweets = retweets + other.retweets, trackterm = other.trackterm)
  override def toJson():String = {
    s"""{
       |    "cnt": ${count},
       |    "${nameID}": ${JsonResult.cleanAndQuote(tweetType)},
       |    "timestamp": ${timestamp},
       |    "retweets": ${retweets},
       |    "trackterm": ${JsonResult.cleanAndQuote(trackterm)}
       |}
      """.stripMargin
  }

  override val key = tweetType + trackterm
  override def total() = count
}

object AVGRetweets {

  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[AVGRetweetsResult], props:TweetProps):Unit = {
    stream
      .filter(t=>t.tweetType == "Retweet")
      .map(t => AVGRetweetsResult("RetweetTweetType",t.date,t.tweetType,t.retweetNumRetweets,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGRetweets())
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Retweets Count")
  }

  def addBatchMetric(batch: DataSet[Tweet], outputFile:String):Unit = {
    batch
      .filter(t=>t.tweetType == "Retweet")
      .map(t => AVGRetweetsResult("RetweetTweetType",t.date,t.tweetType,t.retweetNumRetweets,t.searchTerm,1))
      .groupBy(_.key)
      .reduce(_+_)
      .map(t => (t.timestamp,(t.retweets/t.count).toInt,t.trackterm,1))
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("AVG Retweets")
  }

  def compareMetrictoBatch(stream: DataStream[Tweet], compare:List[(String,String)], sink: SinkFunction[AVGRetweetsResult], props:TweetProps):Unit = {
    stream
      .filter(t=>t.tweetType == "Retweet")
      .map(t => AVGRetweetsResult("RetweetTweetType",t.date,t.tweetType,t.retweetNumRetweets,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGRetweets())
      .map(t=> {
        val x:(String,String) = compare.filter(x=> x._2.equals(t.trackterm))(0)
        AVGRetweetsResult("RetweetTweetType",t.timestamp,t.tweetType,(t.retweets-x._1.toInt).toInt,t.trackterm,1)
      })
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Total Count")
  }

  def addPrediction(stream: DataStream[Tweet], compare:List[(String,String)], sink: SinkFunction[AVGRetweetsResult], props:TweetProps):Unit = {
    stream
      .filter(_.tweetType == "Retweet")
      .map(t => AVGRetweetsResult("RetweetTweetType",t.date,t.tweetType,t.retweetNumRetweets,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGRetweets())
      .map(new AVGRetweetsPred(compare))
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Retweets Count")
  }
}


class AVGRetweetsPred(compare:List[(String,String)]) extends RichMapFunction[AVGRetweetsResult, AVGRetweetsResult] { // predict mean between new and old val
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
  override def map(curr: AVGRetweetsResult): AVGRetweetsResult = {
    var prePred:Int = 0

    for (key <- preds.keySet) {
      if (key == curr.trackterm) {
        prePred = preds.getOrDefault(key,1000)
      }
    }
    // Mean
    val newPred:Int = ((prePred + curr.retweets) / 2).toInt
    preds.put(curr.trackterm, newPred)

    AVGRetweetsResult("RetweetTweetType",curr.timestamp,curr.tweetType,newPred,curr.trackterm+"#prediction",curr.count)
  }
}

class AVGRetweets extends ProcessWindowFunction[AVGRetweetsResult, AVGRetweetsResult, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[AVGRetweetsResult], out: Collector[AVGRetweetsResult]): Unit = {
      val time:Long = context.currentProcessingTime
      val avgres:AVGRetweetsResult = elements.foldLeft(AVGRetweetsResult("RetweetTweetType",time,"Retweet",0,"term",0))(_+_)//((AVGRetweetsResult("RetweetTweetType",time,"Retweet",0,0))((a:AVGRetweetsResult, b:AVGRetweetsResult) => (a+b)))
      val (sum,cnt) = (avgres.retweets,avgres.count)
      out.collect(AVGRetweetsResult(avgres.nameID,avgres.timestamp,avgres.tweetType,sum/cnt,avgres.trackterm,1))
  }
}
