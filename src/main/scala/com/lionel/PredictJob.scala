/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lionel

import java.io.File
import java.util.Properties

import com.github.tototoshi.csv.CSVReader
import com.lionel.metrics._
import com.lionel.streaming.{ElasticKit, StreamUtils, TweetProps}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.api.scala._

/**
 * Skeleton for a Flink Batch Job.
 *
 * For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
object PredictJob {

  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,5000))
    val params = ParameterTool.fromPropertiesFile("twitter.properties")
    val propsTweet = new Properties()
    propsTweet.setProperty(TwitterSource.CONSUMER_KEY, params.get("CONSUMER_KEY"))
    propsTweet.setProperty(TwitterSource.CONSUMER_SECRET, params.get("CONSUMER_SECRET"))
    propsTweet.setProperty(TwitterSource.TOKEN, params.get("TOKEN"))
    propsTweet.setProperty(TwitterSource.TOKEN_SECRET, params.get("TOKEN_SECRET"))
    val props = new TweetProps(windowTime = Time.seconds(60),TwitterSource = propsTweet, searchTerms = "donald trump,joe biden,bernie sanders,elizabeth warren,election2020")

    val stream = StreamUtils.createTwitterStream(env, props)

    // load results from batch job for prediction
    val avgTweetLen = env.readTextFile("batch/avgtweet.csv").map(e => e.split(";")).map(e => (e(1).toInt,e(2))) //load data into tuples
    val avgRetweets = env.readTextFile("batch/avgretweets.csv").map(e => e.split(";")).map(e => (e(1).toInt,e(2))) //load data into tuples

    /**
      * PREDICTING
      */

    AVGTweetLength.addPrediction(stream, avgTweetLen, ElasticKit.createSink[AVGTweetLengthResult]("predtweetlength-idx","predtweetlength-timeline"), props)
    AVGRetweets.addPrediction(stream, avgRetweets, ElasticKit.createSink[AVGRetweetsResult]("predavgretweetscount-idx","predavgretweetscount-timeline"), props)
    RetweetFollowers.addPrediction(stream, ElasticKit.createSink[AVGRetweetFollowersResult]("predretweetsfollowers-idx","predretweetsfollowers-timeline"), props)

    // execute program
    env.execute("Flink Batch Scala API Skeleton")
  }
}
