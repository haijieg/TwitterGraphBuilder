package graphbuilder.twitter
import scala.collection.mutable
import scala.io.Source
import spark.SparkContext

object TextFilter {
 
  val s = Source.fromURL(getClass.getResource("/stopwords.txt"))
  val stopwords = new mutable.HashSet[String]()
  val lines = s.getLines()
  lines foreach (w => stopwords.add(w))
  
  def filter (str: String) : Boolean = {
    str.matches("[a-z]{3,}") && !stopwords.contains(str)
  }
  
  def usage() {
    println ("usage: TextFilter <hostname> <sparkhome> <inputpath> <outputpath>")
  }
  
  def main(args: Array[String]) {
      if (args.length < 4) {
	    usage()
	    sys.exit(1)
	  }	    
      val host = args(0)
      val sparkhome = args(1)
      val inputpath = args(2)
      val outputpath = args(3)
      val spark = new SparkContext(host, "TextFilter", sparkhome,
          List("target/deps.jar", "target/scala-2.9.2/twittergraphbuilder_2.9.2-0.0.1.jar"))
      val src = spark.textFile(inputpath)
      System.out.println("Total input: " + src.count().toInt);
      val target = src.filter(filter)
      System.out.println("Total output: " + target.count().toInt);     
      target.saveAsTextFile(outputpath);
      System.out.println("done")
  }
}