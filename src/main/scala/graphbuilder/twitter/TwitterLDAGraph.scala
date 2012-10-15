package graphbuilder.twitter
import scala.collection.mutable
import scala.io.Source
import spark.SparkContext
import SparkContext._
import scala.collection.mutable.ListBuffer
import spark.RDD

object TwitterLDAGraph {

  def edgeformat (e : (Int, Int, Int)): String = {
    e._1 + "\t" + e._2 + "\t" + e._3
  }
  
  def getEdges (input: String) : mutable.Map[(String, String), Int] = {
    try {
    val Parser = new TweetsJSParser(input) 
      Parser.screenName() match {
        case Some(name) => {
        	val wc = Parser.bagOfWords( TextFilter.filter )
            val edges = wc map { case (word, count) => ((name, word), count) }
        	edges
        }
        case None => mutable.Map[(String, String), Int]()
      }
    } catch {
    	case e => e.printStackTrace(); mutable.Map[(String, String), Int]()
    }
  }

   def usage() {
    println ("usage: TwitterLDAGraph <hostname> <inputpath> <outputpath>")
  }
  
  def main(args: Array[String]) {
	  if (args.length < 3) {
	    usage()
	    sys.exit(1)
	  }	    
      val host = args(0)
      val inputpath = args(1)
      val outputpath = args(2)
      
      val spark = new SparkContext(host, "makeLDGraph", System.getenv("SPARK_HOME"),
	      List("/root/spark/TwitterGraphBuilder.jar"))
	  val file = spark.textFile(inputpath)
	  
	  /* Get Edges */
	  spark.logInfo("Get edges... ")
	  val edgelist = ((file flatMap (getEdges))
			  		.reduceByKey ({_ + _}, 256))
			  		.map {case ((user, word), count) => ((user), (word, count))}
      				.cache()
	  spark.logInfo("Extracted edges: " + (edgelist.count()))
	  
	  spark.logInfo("Get vertices... ")	  
	  val vertexlist = edgelist flatMap{case ((user), (word, count)) => List(user, word)}      				
	  spark.logInfo("Extracted vertices: " + (vertexlist.count()))
		
	  // edgelist.saveAsTextFile(outputpath+"/edges")  
	  // vertexlist.saveAsTextFile(outputpath+"/vertices")
	  
	  /* Build Rawid to Normalized Id map */
	  spark.logInfo("Create vidmap... ")
	  val uniqverts = NormalizeVidMap.uniqIds(spark, vertexlist) cache()
	  val uniqwords  = uniqverts.filter( w => !w.startsWith("@") ) cache()
	  val uniqusers = uniqverts filter {w => w.startsWith("@")} cache()
	  val numwords = uniqwords.count().toInt
	  val numusers = uniqusers.count().toInt
	  spark.logInfo("Unique users: " + numwords)
	  spark.logInfo("Unique words: " + numusers)	  
	  uniqusers saveAsTextFile(outputpath + "/vidmap/users")
	  uniqwords saveAsTextFile(outputpath + "/vidmap/words") 
	
	  // Normalize ids in the edge list
	  spark.logInfo("Create normalized edge... ")
	  val vidmap = spark.parallelize ((uniqwords.collect() ++ uniqverts.collect()) zip (0 to numwords+numusers-1)) cache()
	  //val wordmap = uniqwords.cartesian(spark.parallelize(0 to numwords-1))
	  //val usermap = uniqwords.cartesian(spark.parallelize(numwords to numwords+numusers-1))
	  //val vidmap = wordmap union usermap cache
	  val normalizedEdgeList = NormalizeVidMap.translateEdgeWithData(spark, vidmap, edgelist)
	  (normalizedEdgeList map edgeformat) saveAsTextFile(outputpath +"/normalizedEdges")
	  	  
	  // Save userid and wordid
	  //usermap map {case (user, id) => id + "\t" + user} saveAsTextFile(outputpath + "/vidmap/userid")
	  //wordmap map {case (word, id) => id + "\t" + word} saveAsTextFile(outputpath + "/vidmap/wordid")
	 
	  spark.logInfo("Done")  
	  sys.exit(0)
  }	
}