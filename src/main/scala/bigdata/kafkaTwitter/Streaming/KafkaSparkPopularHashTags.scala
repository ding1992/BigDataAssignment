package bigdata.kafkaTwitter.Streaming

import java.util.HashMap

import bigdata.kafkaTwitter.Streaming.SentimentUtils._
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.storage.StorageLevel
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.elasticsearch.spark._
import org.apache.spark.SparkContext
import com.google.gson.{Gson, JsonObject}
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Locale
import org.json4s._
import org.json4s.jackson.JsonMethods._
import twitter4j.json.DataObjectFactory;

import scala.util.Try

object KafkaSparkPopularHashTags {
  
  //val conf = new SparkConf().setMaster("local[6]").setAppName("Spark Streaming - Kafka Producer - PopularHashTags").set("spark.executor.memory", "1g")
  val conf = new SparkConf().setAppName("Spark Streaming - Kafka Producer - PopularHashTags")

  conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
  
  
  
  val gson = new Gson();
  
  val objectMapper = new ObjectMapper();
  
  def parser(json: String): Map[String, Any] = {
    return parse(json).values.asInstanceOf[Map[String, Any]]
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // Create an array of arguments: zookeeper hostname/ip,consumer group, topicname, num of threads   
    val Array(zkQuorum, group, topics, numThreads) = args

    // Set the Spark StreamingContext to create a DStream for every 2 seconds  
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    // Map each topic to a thread  
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // Map value from the kafka message (k, v) pair      
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val tweets = lines.map(parser)
    
    tweets.filter({t => !(t.exists(_._1 == "producerId"))}).foreachRDD{(rdd, time) =>
       rdd.map(t => 
         {
         Map(
           "user"-> t.get("user").asInstanceOf[Option[Map[String,Any]]].get("screenName"),
           "created_at" -> t.get("createdAt"),
           "location" -> (if (t.getOrElse("geoLocation", null) == null) null else s"${t.get("geoLocation").asInstanceOf[Option[Map[String,Any]]].get("longitude")},${t.get("geoLocation").asInstanceOf[Option[Map[String,Any]]].get("latitude")}"),
           "text" -> t.get("text"),
           "hashtags" -> (if (t.get("hashtagEntities").asInstanceOf[Option[List[Map[String,Any]]]].toList.flatten.size!=0) t.get("hashtagEntities").asInstanceOf[Option[List[Map[String,Any]]]].toList.flatten.map(_.get("text")) else null),
           "retweet" -> t.get("retweetCount"),
           "language" -> t.get("lang"),
           "sentiment" -> detectSentiment(t.get("text").toString()).toString
         )
       }).saveToEs("cars_041919/tweet")
     }

    ssc.start()
    ssc.awaitTermination()
  }
}