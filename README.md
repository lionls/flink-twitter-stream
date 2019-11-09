# bigDATA - Flink Twitter Streaming

This projects analyzes Tweets from Twitter using the Apache Flink Datestreaming Engine, Elasticsearch and Kibana for visualization.
The Datastream manipulation is implemented in Scala.

As keys composite keys were used, to separate the different searchterms: 
```key = normalkey + trackterm```

The collected data is saved in the "./twitterstream" folder.

Here you can see an execution and detailed explanation of the programm: 

https://www.youtube.com/watch?v=rRF9ZqnccBI

### Streaming-Metrics:
* **AVGRetweets** - Average number of retweets per trackterm and timewindow
* **AVGTweetLength** - Average character length of the tweets per trackterm and timewindow
* **CountTrackTerms** - Occurances of trackterm per timewindow
* **HashtagCount** - Most popular hashtags used per trackterm and timewindow
* **PopularUser** - user found in tweets with the most followers by timewindow
* **RetweetFollowers** - Mapping between Retweetcount and Followercount average for tweets by trackterm and timewindow
* **Sentiment** - Sum of Sentiment of Tweets ('POSITIVE','NEUTRAL','NEGATIVE') per trackterm and timewindow
* **SourceCount** - Sum of Devices used for tweeting per trackterm and timewindow
* **TotalCount** - Sum of tweetype per trackterm and timewindow
* **WordCount** - most used words by trackterm in timewindow



### Batch-Metrics:
* **AVGRetweets** - Average number of retweets per trackterm
* **AVGTweetLength** - Average character length of the tweets per trackterm
* **CountTrackTerms** - Occurances of trackterm
* **HashtagCount** - Most popular hashtags used per trackterm
* **PopularUser** - user found in tweets with the most followers
* **Sentiment** - Sum of Sentiment of Tweets ('POSITIVE','NEUTRAL','NEGATIVE') per trackterm
* **SourceCount** - Sum of Devices used for tweeting per trackterm
* **TotalCount** - Sum of tweetype per trackterm
* **WordCount** - most used words by trackterm
* **SourceCount Top N** - Top N Sum of Devices used for tweeting per trackterm
* **TotalCount Top N** - Top N Sum of tweetype per trackterm
* **WordCount Top N** - Top N most used words by trackterm 

The calculated batch results are saved in csv format in folder "./batch".

### Compare-Metrics:
* **AVGRetweets** - Average number of retweets per trackterm subtracted by average retweets calculated by batch 
* **AVGTweetLength** - Average character length of the tweets per trackterm subtracted by average tweetlength calculated by batch 
* **TotalCount** - Sum of tweetype per trackterm subtracted by average totalcount calculated by batch 
* **SourceCount Top N** - Top N Sum of Devices used for tweeting per trackterm loaded from batch compared to whether they are also present in the stream
* **TotalCount Top N** - Top N Sum of tweetype per trackterm loaded from batch compared to whether they are also present in the stream
* **WordCount Top N** - Top N most used words by trackterm loaded from batch compared to whether they are also present in the stream

Comparing the metrics and displaying them in a dashboard it is obvious, that at the first 3 metrics they are more similar if they are closer to 0. At the last 3 metrics they are tracking whether a term occures in batch and in streaming. 


### Prediction-Metrics:
* **AVGRetweets** - Predicting the value of the next timewindow average retweets count
* **AVGTweetLength** - Predicting the value of the next timewindow average tweet length
* **RetweetFollowers** - Predicts the count of retweets for followers on a tweet in average per timewindow

The first 2 metrics are calculated by following formula: 
> nextPrediction = (oldPrediction + currentValue) / 2

The last metric uses a Linear Regression model.

### How to run
First add your own twitter.properties file with your secret keys.

To run the different objectives you first have to launch a docker-elk stack using:
```
docker-compose up
```
Then you can run the specific programms:
```
java -Dmode=2 -DpropertyPath=config.properties -Dpresentation -cp big-data-project-1.0.jar de.bigdata.project.TwitterAnalysisMain

```