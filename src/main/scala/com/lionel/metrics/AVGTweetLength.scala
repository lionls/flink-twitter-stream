package com.lionel.metrics

import java.sql.{Time, Timestamp}
import java.util
import java.util.Calendar

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction

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

  def compareMetrictoBatch(stream: DataStream[Tweet], avgtweetlen: DataStream[(Int, String)], sink: SinkFunction[AVGTweetLengthResult], props:TweetProps):Unit = {
    stream
      .filter(!_.text.isEmpty)
      .filter(_.tweetType == "Original Tweet")
      .map(t => AVGTweetLengthResult(t.date,t.text.length,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGTweetLength())
      .connect(avgtweetlen)
      .keyBy(_.key,_._2)
      .flatMap(new AVGTweetLengthDiff)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Tweet length")
  }

  def addPrediction(stream: DataStream[Tweet], avgtweetlen: DataStream[(Int, String)], sink: SinkFunction[AVGTweetLengthResult], props:TweetProps):Unit = {
    stream
      .filter(!_.text.isEmpty)
      .filter(_.tweetType == "Original Tweet")
      .map(t => AVGTweetLengthResult(t.date,t.text.length,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .process(new AVGTweetLength())
      .connect(avgtweetlen)
      .keyBy(_.key,_._2)
      .flatMap(new AVGTweetLengthPred)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("AVG Tweet length")
  }

}

class AVGTweetLengthDiff extends RichCoFlatMapFunction[AVGTweetLengthResult,(Int,String),AVGTweetLengthResult]{ // already keyed
    private var compare: ValueState[(Int, String)] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      compare = getRuntimeContext.getState(new ValueStateDescriptor[(Int, String)]("avgCompareVal", createTypeInformation[(Int, String)]))
    }

    override def flatMap1(avgtweet: AVGTweetLengthResult, out: Collector[AVGTweetLengthResult]): Unit = {
      val compareVal = compare.value() // extract the value

      if(compareVal._2 == avgtweet.trackterm){
        out.collect(AVGTweetLengthResult(avgtweet.timestamp, avgtweet.length - compareVal._1, avgtweet.trackterm, avgtweet.count))
      }
    }

    override def flatMap2(avgcompare: (Int,String), out: Collector[AVGTweetLengthResult]): Unit = { //load into valueState
      val compareVal = compare.value()

      if(compareVal != null){
        compare.clear()
      }
      compare.update(avgcompare)
      println("pushed into state " + avgcompare)
    }
}

class AVGTweetLengthPred extends RichCoFlatMapFunction[AVGTweetLengthResult,(Int,String),AVGTweetLengthResult] { // predict mean between new and old val
  private var preds: ValueState[(Int, String)] = _

  // get old data
  override def open(config:Configuration ): Unit = {
    super.open(config)
    preds = getRuntimeContext.getState(new ValueStateDescriptor[(Int, String)]("predTweetlen", createTypeInformation[(Int, String)]))
  }

  @throws[Exception]
  override def flatMap1(curr: AVGTweetLengthResult, out: Collector[AVGTweetLengthResult]): Unit = {
    val predVal = preds.value()

    if(predVal._2 == curr.trackterm){
      val newPred:Int = ((predVal._1 + curr.length) / 2).toInt // calculate mean
      preds.update((newPred, curr.trackterm))
      out.collect(AVGTweetLengthResult(curr.timestamp,newPred,curr.trackterm+"#prediction",curr.count))
    }
  }

  override def flatMap2(avgcompare: (Int,String), out: Collector[AVGTweetLengthResult]): Unit = { //load into valueState
    val predsVal = preds.value()

    if(predsVal != null){
      preds.clear()
    }
    preds.update(avgcompare)
    println("pushed into state " + avgcompare)
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
