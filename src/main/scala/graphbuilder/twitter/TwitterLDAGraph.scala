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
  
  
  def vidmapformat (m : (String, Int)) : String = {
    m._2 + "\t" + m._1
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
	  spark.logInfo("Get edges... ")
	  val edgelist = ((file flatMap (getEdges))
			  		.reduceByKey ({_ + _}, 256))
			  		.map {case ((user, word), count) => ((user), (word, count))}
	  spark.logInfo("Extracted edges: " + (edgelist.count()))
	  
	  
	  edgelist.cache()
	  
	  spark.logInfo("Get vertices... ")	  
	  val vertexlist = edgelist flatMap{case ((user), (word, count)) => List(user, word)}	  
	  spark.logInfo("Extracted vertices: " + (vertexlist.count()))
	  // vertexlist.cache()
		
	  /*
	  edgelist.saveAsTextFile(outputpath+"/edges")  
	  vertexlist.saveAsTextFile(outputpath+"/vertices")
	  */
	  
	  // Raw id to int id map
	  spark.logInfo("Create vidmap... ")	  	  
	  val vidmap = NormalizeVidMap.createVidMap(spark, vertexlist)
	  spark.logInfo("Unique vertices: " + (vidmap.count()))
	  // vidmap.cache()
	  vidmap map vidmapformat saveAsTextFile(outputpath+"/vidmap")
	  
	  // Normalize ids in the edge list
	  spark.logInfo("Create normalized edge... ")	  	  
	  val normalizedEdgeList = NormalizeVidMap.translateEdgeWithData(spark, vidmap, edgelist)
	  (normalizedEdgeList map edgeformat) saveAsTextFile(outputpath +"/normalizedEdges")
	  spark.logInfo("Done")
	  
	  sys.exit(0)
  }	
}