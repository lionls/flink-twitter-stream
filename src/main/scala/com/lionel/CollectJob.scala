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

import java.util.Properties

import com.lionel.metrics._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import com.lionel.streaming.{ElasticKit, StreamUtils, TweetProps}
import com.lionel.twitter.Tweet
import com.nwrs.lionel.streaming.JsonResult
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StreamingEndpoint}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * Skeleton for a Flink Streaming Job.
  *
  * For a tutorial how to write a Flink streaming application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution, run
  * 'mvn clean package' on the command line.
  *
  * If you change the name of the main class (with the public static void main(String[] args))
  * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
  */


object CollectJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,5000))

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */




    val params = ParameterTool.fromPropertiesFile("twitter.properties")
    val propsTweet = new Properties()
    propsTweet.setProperty(TwitterSource.CONSUMER_KEY, params.get("CONSUMER_KEY"))
    propsTweet.setProperty(TwitterSource.CONSUMER_SECRET, params.get("CONSUMER_SECRET"))
    propsTweet.setProperty(TwitterSource.TOKEN, params.get("TOKEN"))
    propsTweet.setProperty(TwitterSource.TOKEN_SECRET, params.get("TOKEN_SECRET"))
    val props = new TweetProps(windowTime = Time.minutes(10),TwitterSource = propsTweet, searchTerms = "donald trump,joe bidden,bernie sanders,elizabeth warren,election2020")



    /*
     * Just collect the incoming Stream and save it to file
     */
    val stream = StreamUtils.saveStream(env, props,"./twitterstream")
    // exrecute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
