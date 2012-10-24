package graphbuilder.twitter
import scala.collection.mutable

import spark.SparkContext._
import spark.RDD
import spark.SparkContext
import spark.storage.StorageLevel
import spark.broadcast.Broadcast
object FastLDAGraph {

  def edgeformat (e : (Int, Int, Int)): String = {
    e._1 + "\t" + e._2 + "\t" + e._3
  }
  
  def getEdges (input: String) : mutable.Map[(String, String), Int] = {
    try {
      val Parser = new TweetsJSParser(input) 
      val name = Parser.screenName()
      val wc = Parser.bagOfWords( TextFilter.filter )
      val edges = wc map { case (word, count) => ((name, word), count) }
      edges
    }
    catch {
    	case e => e.printStackTrace(); mutable.Map[(String, String), Int]()
    }
  }

   def usage() {
    println ("usage: TwitterLDAGraph <hostname> <inputpath> <outputpath>")
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
      
      val spark = new SparkContext(host, "makeLDGraph", sparkhome,
          List("target/deps.jar", "target/scala-2.9.2/twittergraphbuilder_2.9.2-0.0.1.jar"))
	  val file = spark.textFile(inputpath)
	  
	  /* Get Edges */
	  System.out.println("Get edges... ")
	  val edgelist = ((file flatMap (getEdges))
			  		.reduceByKey ({_ + _}, 32))
			  		.map {case ((user, word), count) => ((user), (word, count))}
      				.cache()
	  System.out.println("Extracted edges: " + (edgelist.count()))
	  System.out.println("Save edges: " + (edgelist.count()))
	  edgelist.map {case ((user), (word, count)) => user + "\t" + word + "\t" + count}
      		  .saveAsTextFile(outputpath+"/edges")  
	  
	  
	  System.out.println("Get vertices... ")	  
	  val vertexlist = edgelist flatMap{case ((user), (word, count)) => List(user, word)}      				
	  System.out.println("Extracted vertices: " + (vertexlist.count()))
	  // vertexlist.saveAsTextFile(outputpath+"/vertices")
	  
	  /* Build Rawid to Normalized Id map */
	  System.out.println("Create vidmap... ")
	  val uniqverts = NormalizeVidMap.uniqIds(spark, vertexlist) cache()
	  val uniqwords  = uniqverts.filter( w => !w.startsWith("@") ) cache()
	  val uniqusers = uniqverts filter {w => w.startsWith("@")} cache()
	  val numwords = uniqwords.count().toInt
	  val numusers = uniqusers.count().toInt
	  System.out.println("Unique users: " + numwords)
	  System.out.println("Unique words: " + numusers)	  
	  uniqusers saveAsTextFile(outputpath + "/vidmap/users")
	  uniqwords saveAsTextFile(outputpath + "/vidmap/words") 
	
	  /* Normalize edges */
	  System.out.println("Create normalized edge... ")
	  val wordmap = spark.broadcast(((uniqwords.collect()) zip (0 to numwords-1)) toMap)
	  val usermap = spark.broadcast(((uniqusers.collect()) zip (0 to numusers-1)) toMap)
	  val normalizedEdgeList = edgelist map {
	    case ((user), (word, count)) => (usermap.value(user) + numwords, wordmap.value(word), count)
	  }
	  (normalizedEdgeList map edgeformat) saveAsTextFile(outputpath +"/normalizedEdges")
	  	 
	  System.out.println("Done")  
	  sys.exit(0)
  }	
}