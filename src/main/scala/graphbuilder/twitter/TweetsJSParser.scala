package graphbuilder.twitter
import com.codahale.jerkson.Json._
import scala.collection.mutable
import cmu.arktweetnlp.Tagger
import cmu.arktweetnlp.Twokenize
import scala.io.Source
import scala.collection.JavaConversions._

class TweetsJSParser (str: String){
  def rtChain() : List[String] = {
    val extractor = """(RT|via|from|retweet)\s+@(\w+):""".r
    parsedTweet.get("text") match {
      case Some(value:String) => {
    	val iter = extractor.findAllIn(value)
    	val rtfrom = iter.foldLeft(List[String]())((ls, retweet) => retweet match {case extractor(_, user) => "@"+user :: ls})
    	rtfrom
      }
      case None => List[String]()
    }
  }

  def screenName() : Option[String] = {
    parsedTweet.get("user") match {
      case Some(m) => {
        val user = m.asInstanceOf[java.util.LinkedHashMap[String,Any]]
        Some("@"+user.get("screen_name").asInstanceOf[String])
      }
      case None => None //println("User entry not found:\n" + parsedTweet.toString()); None 
    }
  }
  
  def tokenize() : Seq[String] = {
      parsedTweet.get("text") match {
	   case Some(value:String) => {
	     val tokenlist = Twokenize.tokenize(value)
	     tokenlist
	   }
	   case _ => List()
      }
  }
  
  def getText() : String = {
    parsedTweet.get("text") match {
      case Some(v:String) => v
      case _ => "***EMPTYTEXT***"
    }
  }
  
  def bagOfWords(f: String => Boolean) : mutable.Map[String, Int] = {
	  parsedTweet.get("text") match {
	   case Some(value:String) => {
	     val normalizedVal = value.toLowerCase().replaceAll("\\.{2,}", "\\.");
	     val tokenlist = TaggerWrapper.tokenizeAndTag(value)
	     val map = mutable.Map[String, Int]()
	     val it = tokenlist.iterator()
	     while (it.hasNext()) {
	       val taggedtoken = it.next()
	       val tag = taggedtoken.tag
	       val token = taggedtoken.token.toLowerCase()
	       if (((tag equals "n") ||
	           (tag equals "v") ||
	           (tag equals "^")) &&
	           f (token)) {	        
	         map.update(token, (map.getOrElse(token, 0) + 1))
	       }	     
	     }
	     map
	   }
	   case _ => mutable.Map()
	 }
  }
  
  val parsedTweet = parse[Map[String, Any]](str)
}