package com.lionel.metrics

import com.lionel.streaming.TweetProps
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.scala.DataSet
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

case class User(follower_count:Long, username:String, accountname:String, img:String, timestamp:Long)extends JsonResult[User]{
  override def +(other:User):User = this   // remove duplicate
  override val key = username
  override def total() = follower_count.toInt
}


object PopularUser {
  def addMetric(stream: DataStream[Tweet], sink: SinkFunction[User], props:TweetProps):Unit = {
    stream
      .map( t => {
        if (t.retweet)
          User(t.retweetFollowers, t.retweetScreenName, t.retweetAccountName, t.retweetImgUrl, System.currentTimeMillis())
        else
          User(t.followers, t.screenName, t.accountName, t.imgUrl, System.currentTimeMillis())
      })
      .keyBy(_.key)
      .reduce(_ + _)
      .timeWindowAll(props.windowTime)
      .reduce(_+_)
      .addSink(sink)
      .setParallelism(props.parallelism)
      .name("Popular Users")
  }

  def addBatchMetric(batch: DataSet[Tweet], outputFile:String):Unit = {
    batch
      .map( t => {
        if (t.retweet)
          User(t.retweetFollowers, t.retweetScreenName, t.retweetAccountName, t.retweetImgUrl, System.currentTimeMillis())
        else
          User(t.followers, t.screenName, t.accountName, t.imgUrl, System.currentTimeMillis())
      })
      .groupBy(_.key)
      .reduce(_ + _)
      .map(t=>(t.timestamp,t.accountname,t.username,t.follower_count,t.img))
      .writeAsCsv(outputFile,"\n",";",FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      .name("Hashtag Count")
  }
}
