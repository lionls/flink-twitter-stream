package com.lionel.streaming

import java.util.{Collections, Properties}

import com.lionel.twitter.{Tweet, TwitterEntity}
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StreamingEndpoint}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

case class TweetProps(      parallelism:Int=1,
                            windowTime:Time,
                            searchTerms:String="",
                            langs:String="",
                            users:String="",
                            elasticUrl:String="",
                            TwitterSource:Properties=new Properties
                     )

object StreamUtils {

  def createTwitterStream(env:StreamExecutionEnvironment, props: TweetProps):DataStream[Tweet] ={
    val langs:java.util.List[String] = if (!props.langs.isEmpty)
      props.langs.split(",").toSeq.asJava
    else
      Collections.emptyList()

    val searchTerms:java.util.List[String] = if (!props.searchTerms.isEmpty)
      props.searchTerms.split(",").toSeq.asJava
    else
      Collections.emptyList()

    val users:java.util.List[java.lang.Long] = if (!props.users.isEmpty)
      props.users.split(",").toSeq.map(u => Long.box(u.toLong)).asJava
    else
      Collections.emptyList()

    val ts = new TwitterSource(props.TwitterSource)
    ts.setCustomEndpointInitializer(new TwitterSource.EndpointInitializer with Serializable {
      override def createEndpoint(): StreamingEndpoint = {
        val ep = new StatusesFilterEndpoint()
        if (!langs.isEmpty) ep.languages(langs)
        if (!searchTerms.isEmpty) ep.trackTerms(searchTerms)
        if (!users.isEmpty) ep.followings(users)
        return ep
      }
    })

    env.addSource(ts)
      .map[Option[TwitterEntity]](TwitterEntity.fromJson(_))
      .filter(te => te.nonEmpty && te.get.isInstanceOf[Tweet])
      .map(_.get.asInstanceOf[Tweet]):DataStream[Tweet]
  }

  def saveStream(env:StreamExecutionEnvironment,props: TweetProps, path:String):Unit = {
    val langs:java.util.List[String] = if (!props.langs.isEmpty)
      props.langs.split(",").toSeq.asJava
    else
      Collections.emptyList()

    val searchTerms:java.util.List[String] = if (!props.searchTerms.isEmpty)
      props.searchTerms.split(",").toSeq.asJava
    else
      Collections.emptyList()

    val users:java.util.List[java.lang.Long] = if (!props.users.isEmpty)
      props.users.split(",").toSeq.map(u => Long.box(u.toLong)).asJava
    else
      Collections.emptyList()

    val ts = new TwitterSource(props.TwitterSource)
    ts.setCustomEndpointInitializer(new TwitterSource.EndpointInitializer with Serializable {
      override def createEndpoint(): StreamingEndpoint = {
        val ep = new StatusesFilterEndpoint()
        if (!langs.isEmpty) ep.languages(langs)
        if (!searchTerms.isEmpty) ep.trackTerms(searchTerms)
        if (!users.isEmpty) ep.followings(users)
        return ep
      }
    })

    env.addSource(ts)
      .writeAsText(path,WriteMode.OVERWRITE)
  }
}
