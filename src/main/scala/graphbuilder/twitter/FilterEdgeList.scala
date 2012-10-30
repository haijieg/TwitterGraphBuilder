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
object FilterEdgeList {

  def edgeformat (e : (Int, Int, Int)): String = {
    e._1 + "\t" + e._2 + "\t" + e._3
  }

   def usage() {
    println ("usage: FilterEdgeList <hostname> <sparkhome> <inputpath> <outputpath>")
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
      
      val spark = new SparkContext(host, "FilterEdgeList", sparkhome,
          List("target/deps.jar", "target/scala-2.9.2/twittergraphbuilder_2.9.2-0.0.1.jar"))
	
       /* Get Edges */
	  System.out.println("Get edges... ")
	  val edgelist = spark.textFile(inputpath +"/edges").
	  					map { w => {val sp = w.split("\t"); 
	  					if (sp.length == 3) {
	  					    (sp(0), sp(1), sp(2).toInt)
	  					} else {
	  					 System.err.println("Illegal edge: " + w)
	  					 ("**", "**", 0)
	  					}}}.
	  					filter {case (user, word, count) => TextFilter.filter(word) && count > 0}
	  					
	  edgelist.map{case(user, word, count) => user + "\t" + word + "\t" + count}.
	  	saveAsTextFile(outputpath + "/new_edges")
      
	  System.out.println("Get vertices... ")	  
	  val vertexlist = edgelist flatMap{case ((user, word, count)) => List(user, word)}      				
	  System.out.println("Extracted vertices: " + (vertexlist.count()))  
	  /* Build Rawid to Normalized Id map */
	  System.out.println("Create vidmap... ")
	  val uniqverts = NormalizeVidMap.uniqIds(spark, vertexlist) cache()
	  val uniqwords  = uniqverts.filter( w => !w.startsWith("@") ) cache()
	  val uniqusers = uniqverts filter {w => w.startsWith("@")} cache()
	  val numwords = uniqwords.count().toInt
	  val numusers = uniqusers.count().toInt
	  System.out.println("Unique users: " + numusers)
	  System.out.println("Unique words: " + numwords)	  
	  uniqusers saveAsTextFile(outputpath + "/vidmap/users")
	  uniqwords saveAsTextFile(outputpath + "/vidmap/words") 		  	 
	  System.out.println("Done")  
	  sys.exit(0)
  }	
}

