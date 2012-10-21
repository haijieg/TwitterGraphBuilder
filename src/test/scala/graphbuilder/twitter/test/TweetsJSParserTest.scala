package graphbuilder.twitter.test
import org.junit._
import scala.io.Source
import graphbuilder.twitter.TweetsJSParser
class TweetsJSParserTest {
@Test
def test = {
  val lines = Source.fromURL(getClass.getResource("tweets.2012-09-15")).getLines()
  val names = new scala.collection.mutable.ListBuffer[String]()
  lines.foreach (l => {
    try {
    	val parser = new TweetsJSParser(l)    	
    	names += parser.screenName()
    } catch {
      // case e => e.printStackTrace()
      case e => ()
    }
  }
  )
  println(names.toList.length)
}
}