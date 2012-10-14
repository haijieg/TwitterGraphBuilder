package graphbuilder.twitter
import scala.collection.mutable
import scala.io.Source

object TextFilter {
  //val stopwordurl = getClass().getClassLoader().getResource("stopwords.txt")
  /*
  val s = Source.fromFile("/stopwords.txt")
  val stopwords = new mutable.HashSet[String]()
  s.getLines().foreach {
    line => line.split(" ").foreach( word => stopwords.add(word))
  }*/
  
  def filter (str: String) : Boolean = {
    /*
    val ret = str.matches("[a-z]{3,}") && !stopwords.contains(str)
    ret
    */
    str.matches("[a-z]{3,}")
  }
}