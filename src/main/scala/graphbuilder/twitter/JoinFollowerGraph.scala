package graphbuilder.twitter
import scala.collection.mutable
import spark.HashPartitioner
import spark.SparkContext._
import spark.RDD
import spark.SparkContext
import spark.storage.StorageLevel
import spark.broadcast.Broadcast
import spark.HashPartitioner
import spark.HashPartitioner
import scala.collection.mutable.HashMap
object JoinFollowerGraph {
   def usage() {
    println ("usage: JoinFollowerGraph <hostname> <sparkhome> <userfeatures> <followeridpath> <outputpath>")
  }
  
  def main(args: Array[String]) {
	  if (args.length < 5) {
	    usage()
	    sys.exit(1)
	  }	    
      val host = args(0)
      val sparkhome = args(1)
      val inputpath = args(2)
      val followeridpath = args(3)
      val outputpath = args(4)
      val spark = new SparkContext(host, "FollowerJoinLda", sparkhome,
          List("target/deps.jar", "target/scala-2.9.2/twittergraphbuilder_2.9.2-0.0.1.jar"))
      
      // Get user features      
      System.out.println("Get user features...")
      val userfeatures = spark.textFile(inputpath).map(w => {val sp = w.split("\t"); (sp.head, sp.tail)})
      System.out.println("User feature record: " + userfeatures.count())
                 
      // Get follower id map
      System.out.println("Get follower ids...")
      val followeridmap = spark.textFile(followeridpath).map(w => {val sp = w.split(" "); ("@"+sp(1), sp(0))})
      
      // Broadcast follower id map
      System.out.println("Broadcast maps...")      
      val followeridmapbc = spark.broadcast(followeridmap.collectAsMap())
      
      System.out.println("Map ids...")
      // map features using id map
      val mappedfeatures = userfeatures.filter{
        case (name, features) => followeridmapbc.value.contains(name)
      }.map {
        case (name, features) => followeridmapbc.value(name) + "\t" +
        		features.foldLeft("")( (x,y) => x + "\t" + y)
      }
      System.out.println("Final records: " + mappedfeatures.count())
      mappedfeatures.saveAsTextFile(outputpath)
      System.out.println("done")
  }	
}

