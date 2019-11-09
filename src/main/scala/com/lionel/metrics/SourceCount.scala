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

/**
  *
  * counts by source - iphone android ipad etc
  */

case class SourceResult(source:String, sourceUrl:String, timestamp:Long, searchTerm:String, cnt:Int) extends JsonResult[SourceResult] {
  override def +(other: SourceResult): SourceResult = this.copy(cnt = other.cnt + cnt)
  override def key() = source + searchTerm // use composite key to get every single value
  override def total() = cnt

  override def toJson():String = {
    s"""{
       |    "cnt": ${cnt},
       |    "source": ${JsonResult.cleanAndQuote(source)},
       |    "timestamp": ${timestamp},
       |    "searchTerm":${JsonResult.cleanAndQuote(searchTerm)}
       |}
      """.stripMargin
  }

}

object SourceCount {
  val sourceRegex = """<a href="([^"]+)"([^>]*)>(.+?)</a>""".r

  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[SourceResult], props:TweetProps):Unit = {
    stream
      .filter(_.source.length > 0)
      .map(t => t.source match {
        case sourceRegex(link,_,app) => Some(SourceResult(app,link,t.date, t.searchTerm,1))
        case _ => None
      }
      )
      .filter(_.nonEmpty)
      .map(_.get)
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Source Count Disjunct")
  }

  def addBatchMetric(batch: DataSet[Tweet], outputFile:String):Unit = {
    batch
      .filter(_.source.length > 0)
      .map(t => t.source match {
        case sourceRegex(link,_,app) => Some(SourceResult(app,link,t.date, t.searchTerm,1))
        case _ => None
      }
      )
      .filter(_.nonEmpty)
      .map(_.get)
      .groupBy(_.key)
      .reduce(_ + _)
      .filter(_.cnt>2) // ignore all the too small tools
      .map(t=>(t.timestamp,t.searchTerm,t.source,t.cnt))
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Source Count")
  }

  def addBatchMetricTopN(batch: DataSet[Tweet], n:Int, outputFile:String):Unit = {
    batch
      .filter(_.source.length > 0)
      .map(t => t.source match {
        case sourceRegex(link,_,app) => Some(SourceResult(app,link,t.date, t.searchTerm,1))
        case _ => None
      }
      )
      .filter(_.nonEmpty)
      .map(_.get)
      .map(t=>(t.timestamp,t.source,t.searchTerm,t.cnt))
      .groupBy(1, 2)
      .sum(3)
      .groupBy(2)
      .sortGroup(3, Order.DESCENDING)
      .first(n)
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Source Count")
  }

  def compareMetrictoBatch(stream: DataStream[Tweet], compare:List[(String,String,Long)], sink: SinkFunction[SourceResult], props:TweetProps):Unit = {
    stream
      .filter(_.source.length > 0)
      .map(t => t.source match {
        case sourceRegex(link,_,app) => Some(SourceResult(app,link,t.date, t.searchTerm,1))
        case _ => None
      }
      )
      .filter(_.nonEmpty)
      .map(_.get)
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .map(t=> {
          val arr = compare.filter(c=> t.searchTerm == c._1 && t.source == c._2)
          var value:Int = 0
          if(arr.length>0){
            value = arr(0)._3.toInt
          }

          SourceResult(t.source,t.sourceUrl,t.timestamp, t.searchTerm,t.cnt-value)
        })
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Source Count Disjunct")

  }

}
