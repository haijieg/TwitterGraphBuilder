package graphbuilder.twitter
import scala.collection.mutable
import cmu.arktweetnlp.Tagger
import cmu.arktweetnlp.Twokenize
import scala.io.Source
import scala.collection.JavaConversions._
import net.minidev.json.JSONValue

class TweetsJSParser (str: String){
  def rtChain() : List[String] = {
    val extractor = """(RT|via|from|retweet)\s+@(\w+):""".r
    val text = getText()
    val iter = extractor.findAllIn(text)
   	val rtfrom = iter.foldLeft(List[String]())((ls, retweet) => retweet match {case extractor(_, user) => "@"+user :: ls})
    rtfrom
  }
  
  def getText() : String = {
    parsedTweet.get("text").toString()
  }
  
  def getRawText() : String = str

  def screenName() : String = {
    "@"+parsedTweet.get("user").get("screen_name").toString()
  }
  
  def tokenize() : Seq[String] = {
      Twokenize.tokenize(getText())
  }  
  
  def bagOfWords(f: String => Boolean) : mutable.Map[String, Int] = {
	     val text = getText()
	     val normalizedVal = text.toLowerCase().replaceAll("\\.{2,}", "\\.");
	     val tokenlist = TaggerWrapper.tokenizeAndTag(text)
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
  
  val parsedTweet = new ScalaJSON(JSONValue.parse(str))  
}