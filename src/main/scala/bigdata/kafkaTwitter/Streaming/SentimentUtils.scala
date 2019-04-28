package bigdata.kafkaTwitter.Streaming

import java.util.Properties
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

object SentimentUtils {

  val nlpProperties = {
  
    val properties = new Properties()
    /* Annotators - Meaning http://corenlp.run/
       tokenize   - Tokenize the sentence.
       ssplit     - Split the text into sentence. Identify fullstop, exclamation etc and split sentences
       pos        - Reads text in some language and assigns parts of speech to each word (and other token), such as noun, verb, adjective, etc.
       lemma      - Group together the different inflected forms of a word so they can be analysed as a single item.
       parse      - Provide syntactic analysis http://nlp.stanford.edu:8080/parser/index.jsp
       sentiment  - Provide model for sentiment analysis
       * */
    
	properties.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    properties
	
  }

  def detectSentiment(message: String): String = {

    // Create a pipeline with NLP properties
    val pipeline = new StanfordCoreNLP(nlpProperties)

    // Run message through the Pipeline
    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var main_sentiments = 0

    // An Annotation is a Map and you can get and use the various analyses individually.
    // For instance, this gets the parse tree of the first sentence in the text.
    // Iterate through tweet
    for (tweet_msg <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
	
      // Create a RNN parse tree
	  val parse_tree = tweet_msg.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      
	  // Detect Sentiment
      val tweet_sentiments = RNNCoreAnnotations.getPredictedClass(parse_tree)
      val part_text = tweet_msg.toString
weightedSentiment
      if (part_text.length() > longest) {
        main_sentiments = tweet_sentiments
        longest = part_text.length()
      }

      sentiments += tweet_sentiments.toDouble
      sizes += part_text.length
	  
    }

    val weighted_sentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weighted_sentiment = weighted_sentiments.sum / (sizes.fold(0)(_ + _))

    if (weighted_sentiment <= 0.0)
      "NOT_UNDERSTOOD"
    else if (weighted_sentiment < 1.6)
      "NEGATIVE"
    else if (weighted_sentiment <= 2.0)
      "NEUTRAL"
    else if (weighted_sentiment < 5.0)
      "POSITIVE"
    else "NOT_UNDERSTOOD"    
  }
}