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

import com.lionel.metrics._
import com.lionel.streaming.ElasticKit
import com.lionel.twitter.{Tweet, TwitterEntity}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

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
object BatchJob {

  def main(args: Array[String]) {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataSet[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/apis/batch/index.html
     *
     * and the examples
     *
     * http://flink.apache.org/docs/latest/apis/batch/examples.html
     *
     */


    val stream = env.readTextFile("./twitterstream")

    val loaded = stream
      .map[Option[TwitterEntity]](TwitterEntity.fromJson(_))
      .filter(te => te.nonEmpty && te.get.isInstanceOf[Tweet])
      .map(_.get.asInstanceOf[Tweet])



    // push tweet count per Time Window to elastic

    TotalCount.addBatchMetric(loaded,"batch/totalcount.csv")

    // push avg likes per Time Window to elastic

    AVGRetweets.addBatchMetric(loaded,"batch/avgretweets.csv")

    // push sentiment for Time Window seperating count for POSITIVE, NEGATIVE, NEUTRAL

    Sentiment.addBatchMetric(loaded,"batch/sentiment.csv")

    // push hashtag counter for Time Window

    HashtagCount.addBatchMetric(loaded,"batch/hashtag.csv")

    // get popular users for topic

    PopularUser.addBatchMetric(loaded,"batch/popularuser.csv")

    // count occurances of track Terms

    CountTrackTerms.addBatchMetric(loaded, "batch/trackterms.csv")

    // track disjunct use of devices

    SourceCount.addBatchMetric(loaded, "batch/sourcecount.csv")

    // Word Count per tag

    WordCount.addBatchMetric(loaded, "batch/wordcount.csv") //get all

    // AVG Tweets per Trackterm

    AVGTweetLength.addBatchMetric(loaded, "batch/avgtweet.csv")



    // BATCH get the same with less values for compare Function only TopN

    WordCount.addBatchMetricTopN(loaded, 7, "batch/wordcountTopN.csv") // get top N for compare function
    HashtagCount.addBatchMetricTopN(loaded, 10,"batch/hashtagTopN.csv")
    SourceCount.addBatchMetricTopN(loaded, 6,"batch/sourcecountTopN.csv")
    // execute program
    env.execute("Flink Batch Scala API Skeleton")
  }
}
