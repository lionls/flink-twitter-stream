package com.lionel.twitter

import java.util.Date

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import scala.collection.JavaConverters._
import scala.io.Source
import twitter4j.TwitterObjectFactory._

// Tweet case class, analytic stream operations work on instances of this
case class Tweet(accountName:String="",
                 screenName:String="",
                 searchTerm:String="",
                 text:String="",
                 imgUrl:String="",
                 date:Long=new Date().getTime,
                 retweet:Boolean=false,
                 followers:Long=0,
                 userId:Long=0,
                 verified:Boolean=false,
                 profileText:String="",
                 profileLocation:String="",
                 inReplyTo:Long=0,
                 hasLinks:Boolean=false,
                 links:String="",
                 hasHashtags:Boolean=false,
                 hashtags:Array[String]=Array[String](),
                 favorite_count:Int = 0,
                 retweet_count:Int = 0,
                 retweetAccountName:String="",
                 retweetScreenName:String="",
                 retweetNumRetweets:Long=0,
                 retweetFollowers:Long=0,
                 retweetImgUrl:String="",
                 tweetType:String="",
                 source:String="",
                 id:Long=0,
                 retweetId:Long=0,
                 profileTown: String="",
                 profileArea: String="",
                 profileRegion: String="",
                 profileCountry: String="",
                 locationGeo:String="",
                 locationAccuracy: Int=0,
                 iso31662: String="",
                 gender:String="") extends TwitterEntity

object Tweet {

  lazy val log:Logger = LoggerFactory.getLogger(Tweet.getClass)

  def fromJson(twitterJson:String):Option[Tweet] = {
      try {
        val t = createStatus(twitterJson)


        val isRetweet = t.isRetweet
        val text = if (isRetweet)
          t.getRetweetedStatus.getText
        else
          t.getText

        val entities = t.getURLEntities
        val hasLinks = (entities != null & entities.length > 0)
        val links = if (hasLinks)
          entities.toSeq.map(_.getExpandedURL).mkString(" ")
        else
          ""

        val htEntities = t.getHashtagEntities
        val hasHashtags = (htEntities != null & htEntities.length > 0)
        val hashtags = if (hasHashtags)
          htEntities.toSeq.map(_.getText).toArray
        else
          Array[String]()

        val u = t.getUser


        // map the right trackterm for search results

        val searchTerms = "donald trump,joe biden,bernie sanders,elizabeth warren,election2020".split(",")
        val MainText = if (text !=null) text.toLowerCase else ""
        val QuotedText = if (t.getQuotedStatus != null && t.getQuotedStatus.getText != null) t.getQuotedStatus.getText.toLowerCase else ""
        val textA = MainText + QuotedText //scan through quote if tag not appears in main text

        val allTags = List("donald","trump","joe","biden","bernie","sanders","elizabeth","warren","election2020")

        val inside = allTags.filter(t => textA.contains(t))
        val term = inside match{
          case List("donald",_*) => "trump"
          case List("joe",_*) => "biden"
          case List("bernie",_*) => "sanders"
          case List("elizabeth",_*) => "warren"
          case List("election2020",_*) => "election2020"
          case List(a,_*) => a
          case List(a) => a
          case _ => "election2020"
        }

        Some(Tweet(accountName = u.getName,
          screenName = u.getScreenName,
          searchTerm = term,
          text = if (text !=null) text else "",
          imgUrl = if (u.getProfileImageURL !=null) u.getProfileImageURL else "",
          date = t.getCreatedAt.getTime,
          retweet = isRetweet,
          followers = u.getFollowersCount,
          userId = u.getId,
          verified = u.isVerified,
          profileText = if (u.getDescription !=null) u.getDescription else "",
          profileLocation = if (u.getLocation !=null) u.getLocation else "",
          inReplyTo = t.getInReplyToStatusId,
          hasLinks = hasLinks,
          links = links,
          hasHashtags = hasHashtags,
          hashtags = hashtags,
          favorite_count = t.getFavoriteCount(),
          retweet_count = t.getRetweetCount(),
          retweetAccountName = if (isRetweet) t.getRetweetedStatus.getUser.getName else "",
          retweetScreenName = if (isRetweet) t.getRetweetedStatus.getUser.getScreenName else "",
          retweetNumRetweets = if (isRetweet) t.getRetweetedStatus.getRetweetCount else 0,
          retweetFollowers = if (isRetweet) t.getRetweetedStatus.getUser.getFollowersCount else 0, //getFavouritesCount
          retweetImgUrl = if (isRetweet) t.getRetweetedStatus.getUser.getProfileImageURL else "",
          tweetType = if (isRetweet) "Retweet" else if (t.getInReplyToStatusId > 0) "Reply" else "Original Tweet",
          source = if (t.getSource!=null) t.getSource else "",
          id = t.getId,
          retweetId = if (isRetweet) t.getRetweetedStatus.getId else 0
        ))
      } catch {
        case t: Throwable => {
          log.error("Failed to create tweet from JSON: ", t)
          return None
        }
      }
  }


}
