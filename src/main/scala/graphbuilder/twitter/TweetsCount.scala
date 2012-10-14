package graphbuilder.twitter
import scala.collection.mutable
import scala.io.Source
import spark.SparkContext
import SparkContext._
import scala.collection.mutable.ListBuffer
import spark.RDD

object TweetsCount {
  
def usage() {
  println ("usage: TweetsCount <hostname> <inputpath> <outputpath>")
}

  
def main(args: Array[String]) {
	  if (args.length < 3) {
	    usage()
	    sys.exit(1)
	  }	    
      val host = args(0)
      val inputpath = args(1)
      val outputpath = args(2)

	  val spark = new SparkContext(host, "TweetsCount")
	  val file = spark.textFile(inputpath)
	  println ("Total lines: " + file.count())
	  sys.exit(0)
  }	
}