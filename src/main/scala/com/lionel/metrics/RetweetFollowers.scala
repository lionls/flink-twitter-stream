package com.lionel.metrics

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
import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}

import scala.collection.mutable
import scala.collection.convert.wrapAll._
import scala.collection.mutable


case class AVGRetweetFollowersResult(followers:Long, timestamp:Long, retweets:Int, trackterm:String, count:Int ) extends JsonResult[AVGRetweetFollowersResult] {
  override def +(other:AVGRetweetFollowersResult):AVGRetweetFollowersResult = this.copy(count = count + other.count, retweets = retweets + other.retweets, trackterm = other.trackterm,followers = followers + other.followers)
  override def toJson():String = {
    s"""{
       |    "cnt": ${count},
       |    "followers": ${followers},
       |    "timestamp": ${timestamp},
       |    "retweets": ${retweets},
       |    "trackterm": ${JsonResult.cleanAndQuote(trackterm)}
       |}
      """.stripMargin
  }

  override val key = trackterm
  override def total() = count
}

object RetweetFollowers {

  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[AVGRetweetFollowersResult], props:TweetProps):Unit = {
    stream
      .filter(_.retweet)
      .map(t =>AVGRetweetFollowersResult(t.retweetFollowers, t.date, t.retweetNumRetweets.toInt, t.searchTerm, 1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGRetweetFollowers())
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Retweets Followers Count")
  }

  def addPrediction(stream: DataStream[Tweet], sink: SinkFunction[AVGRetweetFollowersResult], props:TweetProps):Unit = {
    stream
      .filter(_.retweet)
      .map(t =>AVGRetweetFollowersResult(t.retweetFollowers, t.date, t.retweetNumRetweets.toInt, t.searchTerm, 1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGRetweetFollowers())
      .keyBy(_.key)
      .map(new AVGRetweetsFollowersPred())
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Retweets Followers prediction Count")
  }
}

class AVGRetweetFollowers extends ProcessWindowFunction[AVGRetweetFollowersResult, AVGRetweetFollowersResult, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[AVGRetweetFollowersResult], out: Collector[AVGRetweetFollowersResult]): Unit = {
    val time:Long = context.currentProcessingTime

    val avgres:AVGRetweetFollowersResult = elements.foldLeft(AVGRetweetFollowersResult(0,time,0,"term",0))(_+_)
    val (sumRetweets,sumFollowers,cnt) = (avgres.retweets,avgres.followers,avgres.count)
    out.collect(AVGRetweetFollowersResult((avgres.followers/cnt).toInt, time, (avgres.retweets/cnt).toInt, avgres.trackterm, 1))
  }
}


class AVGRetweetsFollowersPred extends RichMapFunction[AVGRetweetFollowersResult, AVGRetweetFollowersResult] { // predict using linear regression
    var model:ValueState[SimpleRegression] = _

  override def open(config:Configuration ): Unit = { //init tags
    model = getRuntimeContext.getState(new ValueStateDescriptor("model",classOf[SimpleRegression]))
  }

  @throws[Exception]
  override def map(curr: AVGRetweetFollowersResult): AVGRetweetFollowersResult = {
    var prediction:Double = 0
    var modelExtracted = model.value()

    if(modelExtracted == null){
      modelExtracted = new SimpleRegression()
    }

    try {
      prediction = modelExtracted.predict(curr.followers)
    } catch {
      case _:Throwable => println("error in prediction")
    }

    modelExtracted.addData(curr.followers.toDouble,curr.retweets.toDouble)
    model.update(modelExtracted)
    AVGRetweetFollowersResult(curr.followers, curr.timestamp, prediction.toInt, curr.trackterm + "#prediction", 1)
  }
}


//class AVGRetweetsFollowersPred extends RichMapFunction[AVGRetweetFollowersResult, AVGRetweetFollowersResult] { // predict using linear regression
//
//  var predTrump:ValueState[SimpleRegression] = null
//  var predWarren:ValueState[SimpleRegression] = null
//  var predBiden :ValueState[SimpleRegression] = null
//  var predSanders:ValueState[SimpleRegression] = null
//  var predElection:ValueState[SimpleRegression] = null
//
//
//  override def open(config:Configuration ): Unit = { //init tags
//    predTrump = getRuntimeContext.getState(new ValueStateDescriptor("trump",classOf[SimpleRegression]))
//    predWarren = getRuntimeContext.getState(new ValueStateDescriptor("warren",classOf[SimpleRegression]))
//    predBiden = getRuntimeContext.getState(new ValueStateDescriptor("biden",classOf[SimpleRegression]))
//    predSanders = getRuntimeContext.getState(new ValueStateDescriptor("sanders",classOf[SimpleRegression]))
//    predElection = getRuntimeContext.getState(new ValueStateDescriptor("election",classOf[SimpleRegression]))
//  }
//
//  @throws[Exception]
//  override def map(curr: AVGRetweetFollowersResult): AVGRetweetFollowersResult = {
//    var prediction:Double = 0
//    var model:ValueState[SimpleRegression] = null
//
//    curr.trackterm match{
//      case "trump" => {model = predTrump}
//      case "warren" => {model = predWarren}
//      case "biden" => {model = predBiden}
//      case "sanders" => {model = predSanders}
//      case "election2020" => {model = predElection}
//      case _ => println("not found")
//    }
//
//
//    var modelExtracted = model.value()
//
//    if(modelExtracted == null){
//      modelExtracted = new SimpleRegression()
//    }
//
//    try {
//      prediction = modelExtracted.predict(curr.followers)
//      println("got "+ prediction)
//    } catch {
//      case _:Throwable => println("error in prediction")
//    }
//
//    modelExtracted.addData(curr.followers.toDouble,curr.retweets.toDouble)
//    println(prediction + " updated model " + curr.retweets)
//    model.update(modelExtracted)
//    println(modelExtracted)
//
//    AVGRetweetFollowersResult(curr.followers, curr.timestamp, prediction.toInt, curr.trackterm + "#prediction", 1)
//  }
//}