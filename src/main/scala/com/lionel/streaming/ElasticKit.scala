package com.lionel.streaming

import com.nwrs.lionel.streaming.JsonResult
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchApiCallBridge, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.elasticsearch.client.{Requests, RestClientBuilder}
import org.elasticsearch.common.xcontent.XContentType

object ElasticKit {
  val indices = Seq("count-idx",
    "gender-idx",
    "geo-idx",
    "hashtags-idx",
    "links-idx",
    "profile-bigrams-idx",
    "profile-topics-idx",
    "retweets-idx",
    "influencers-idx")

  def createSink[T <: JsonResult[_]](idx:String, mapping:String) = {
    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))
    val sinkFunc = new ElasticsearchSinkFunction[T] {
      override def process(cc: T, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        requestIndexer.add(Requests.indexRequest()
          .index(idx)
          .`type`(mapping)
          .source(cc.toJson, XContentType.JSON))
      }
    }
    import collection.JavaConverters._
    val esSinkBuilder = new ElasticsearchSink.Builder[T](httpHosts,sinkFunc)
    esSinkBuilder.setBulkFlushMaxActions(50)
    esSinkBuilder.setBulkFlushInterval(30000)
    esSinkBuilder.setBulkFlushBackoffRetries(5)
    esSinkBuilder.setBulkFlushBackoffDelay(2000)
    esSinkBuilder.setRestClientFactory(new RestClientFactory {
      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
        // deactivated auth on the server - it's easier ;P
      }
    })
    esSinkBuilder.build()
  }

//  def createOutputFormat[T <: JsonResult[_]](idx:String, mapping:String)

}
