package com.lionel.metrics

import java.sql.{Time, Timestamp}
import java.util.Calendar

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
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

  def compareMetrictoBatch(stream: DataStream[Tweet], avgRetweets: DataStream[(Int, String)], sink: SinkFunction[AVGRetweetsResult], props:TweetProps):Unit = {
    stream
      .filter(t=>t.tweetType == "Retweet")
      .map(t => AVGRetweetsResult("RetweetTweetType",t.date,t.tweetType,t.retweetNumRetweets,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGRetweets())
      .connect(avgRetweets)
      .keyBy(_.trackterm,_._2)
      .flatMap(new AVGRetweetsDiff)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Total Count")
  }

  def addPrediction(stream: DataStream[Tweet], avgRetweets: DataStream[(Int, String)], sink: SinkFunction[AVGRetweetsResult], props:TweetProps):Unit = {
    stream
      .filter(_.tweetType == "Retweet")
      .map(t => AVGRetweetsResult("RetweetTweetType",t.date,t.tweetType,t.retweetNumRetweets,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGRetweets())
      .connect(avgRetweets)
      .keyBy(_.trackterm,_._2)
      .flatMap(new AVGRetweetsPred)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Retweets Count")
  }
}

class AVGRetweetsDiff extends RichCoFlatMapFunction[AVGRetweetsResult,(Int,String),AVGRetweetsResult]{ // already keyed
  private var compare: ValueState[(Int, String)] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    compare = getRuntimeContext.getState(new ValueStateDescriptor[(Int, String)]("avgRetweetVal", createTypeInformation[(Int, String)]))
  }

  override def flatMap1(avgtweet: AVGRetweetsResult, out: Collector[AVGRetweetsResult]): Unit = {
    val compareVal = compare.value() // extract the value

    if(compareVal._2 == avgtweet.trackterm){
      out.collect(AVGRetweetsResult(avgtweet.nameID, avgtweet.timestamp, avgtweet.tweetType, avgtweet.retweets - compareVal._1, avgtweet.trackterm, avgtweet.count))
    }
  }

  override def flatMap2(avgcompare: (Int,String), out: Collector[AVGRetweetsResult]): Unit = { //load into valueState
    val compareVal = compare.value()

    if(compareVal != null){
      compare.clear()
    }
    compare.update(avgcompare)
    println("pushed into state " + avgcompare)
  }
}


class AVGRetweetsPred extends RichCoFlatMapFunction[AVGRetweetsResult,(Int,String),AVGRetweetsResult] { // predict mean between new and old val
  private var preds: ValueState[(Int, String)] = _

  // get old data
  override def open(config:Configuration ): Unit = {
    super.open(config)
    preds = getRuntimeContext.getState(new ValueStateDescriptor[(Int, String)]("predRetweets", createTypeInformation[(Int, String)]))
  }

  @throws[Exception]
  override def flatMap1(curr: AVGRetweetsResult, out: Collector[AVGRetweetsResult]): Unit = {
    val predVal = preds.value()

    if(predVal._2 == curr.trackterm){
      val newPred:Int = ((predVal._1 + curr.retweets) / 2).toInt // calculate mean
      preds.update((newPred, curr.trackterm))
      out.collect(AVGRetweetsResult(curr.nameID,curr.timestamp,curr.tweetType,newPred,curr.trackterm+"#prediction",curr.count))
    }
  }

  override def flatMap2(avgcompare: (Int,String), out: Collector[AVGRetweetsResult]): Unit = { //load into valueState
    val predsVal = preds.value()

    if(predsVal != null){
      preds.clear()
    }
    preds.update(avgcompare)
    println("pushed into state " + avgcompare)
  }
}

class AVGRetweets extends ProcessWindowFunction[AVGRetweetsResult, AVGRetweetsResult, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[AVGRetweetsResult], out: Collector[AVGRetweetsResult]): Unit = {
      val time:Long = context.currentProcessingTime
      val avgres:AVGRetweetsResult = elements.foldLeft(AVGRetweetsResult("RetweetTweetType",time,"Retweet",0,"term",0))(_+_)
      val (sum,cnt) = (avgres.retweets,avgres.count)
      out.collect(AVGRetweetsResult(avgres.nameID,avgres.timestamp,avgres.tweetType,sum/cnt,avgres.trackterm,1))
  }
}
