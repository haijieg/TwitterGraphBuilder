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
      Some(Parser.screenName)
   } catch {
     case e => e.printStackTrace()
     None
   }
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
	  val spark = new SparkContext(host, "UserTweetsCount", sparkhome,
          List("target/deps.jar", "target/scala-2.9.2/twittergraphbuilder_2.9.2-0.0.1.jar"))  
	  val file = spark.textFile(inputpath)
	  val counts = file map {
        line => {
          getScreenName(line) match {
            case Some(name) => (name, 1)
            case None => ("@***", 0)
          }
        }
      } reduceByKey(_ + _, 32)  filter { case (user, count) => count > 3}
      counts map mapformat saveAsTextFile(outputpath)
      System.out.println("done")	
	  sys.exit(0)
  }	
}