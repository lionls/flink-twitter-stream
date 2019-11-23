package com.lionel.metrics



import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.operators.ScalaCsvOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._
import scala.util.control.Breaks.{break, breakable}

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

  def compareMetrictoBatch(stream: DataStream[Tweet], count:DataStream[(String,String,Long)], sink: SinkFunction[TotalCountResult], props:TweetProps):Unit = {
    stream
      .map(t => TotalCountResult("tweetType",t.date,t.tweetType,t.searchTerm,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .connect(count)
      .keyBy(_.trackterm,_._1)
      .flatMap(new CountDiff)
//      .map(t=> {
//        val x:(String,String,Long) = compare.filter(x=> (x._1.equals(t.trackterm) && x._2.equals(t.tweetType)))(0)
//        TotalCountResult("tweetType", t.timestamp, t.tweetType, t.trackterm, (t.count-x._3).toInt)
//      })
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Total Count")
  }

}


class CountDiff extends RichCoFlatMapFunction[TotalCountResult,(String,String,Long),TotalCountResult]{ // already keyed
  private var compare: ListState[(String,String,Long)] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    compare = getRuntimeContext.getListState(new ListStateDescriptor[(String,String,Long)]("countDiff", createTypeInformation[(String,String,Long)]))
  }

  override def flatMap1(t: TotalCountResult, out: Collector[TotalCountResult]): Unit = {
    breakable {
      for (elem:(String,String,Long) <- compare.get) {
        if (elem._2 == t.tweetType && elem._1 == t.trackterm) {
          out.collect(TotalCountResult(t.nameID, t.timestamp, t.tweetType, t.trackterm, (t.count-elem._3).toInt))
          break
        }
      }
    }
  }

  override def flatMap2(elem: (String,String,Long), out: Collector[TotalCountResult]): Unit = { //load into listState
    compare.add(elem)
    println("pushed into state " + elem)
  }
}

