package graphbuilder.twitter
import scala.collection.mutable
import scala.io.Source
import spark.SparkContext
import SparkContext._
import scala.collection.mutable.ListBuffer
import spark.RDD

object TwitterUserTweetsCount {
def mapformat (m : (String, Int)) : String = {
  m._1 + "\t" + m._2
}

def usage() {
  println ("usage: TwitterUserTweetsCount <hostname> <inputpath> <outputpath>")
}

def getScreenName(str: String) : Option[String] = {
   try {
      val Parser = new TweetsJSParser(str) 
      Parser.screenName()
   } catch {
     case e => e.printStackTrace()
     None
   }
}
  
def main(args: Array[String]) {
	  if (args.length < 3) {
	    usage()
	    sys.exit(1)
	  }	    
      val host = args(0)
      val inputpath = args(1)
      val outputpath = args(2)
	  val spark = new SparkContext(host, "UserTweetsCount")  
	  val file = spark.textFile(inputpath)
	  val counts = file map {
        line => {
          getScreenName(line) match {
            case Some(name) => (name, 1)
            case None => ("@***", 0)
          }
        }
      } reduceByKey(_ + _, 256) // filter { case (user, count) => count > 10}
      counts map mapformat saveAsTextFile(outputpath)
      System.out.println("done")	
	  sys.exit(0)
  }	
}