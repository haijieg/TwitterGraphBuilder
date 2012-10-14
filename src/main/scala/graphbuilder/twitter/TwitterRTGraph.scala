package graphbuilder.twitter
import spark.SparkContext
import SparkContext._
import scala.collection.mutable.ListBuffer
import spark.RDD

object TwitterRTGraph {
  def edgeformat (e : (Int, Int)): String = {
    e._1 + "\t" + e._2
  }
  
  def getEdges(input: String) : List[(String, String)] = {
    try {
    val Parser = new TweetsJSParser(input) 
      Parser.screenName() match {
        case Some(name) => {
          val rtchain = Parser.rtChain()
          var previous = name
          val edges = new ListBuffer[(String, String)]
	      rtchain foreach {
            cur => edges += ((previous, cur))
            previous = cur
          }
         edges.toList
        }
        case None => List()
      }
    } catch {
    	case e => e.printStackTrace(); List()
    }
  }
  
  def getVertices(input: String) : List[String] = {
    try {
    val Parser = new TweetsJSParser(input) 
      Parser.screenName() match {
        case Some(name) => {
          val rtchain = Parser.rtChain()
	      if (rtchain.isEmpty)
	        List()
	      else
	        name :: rtchain
        }
        case None => List()
      }
    } catch {
       	case e => e.printStackTrace(); List()
    }
  }   
  def usage() {
    println ("usage: TwitterRTGraph <hostname> <inputpath> <outputpath>")
  }
  
  def main(args: Array[String]) {
	  if (args.length < 3) {
	    usage()
	    sys.exit(1)
	  }	    
      val host = args(0)
      val inputpath = args(1)
      val outputpath = args(2)
      // val inputpath = "/Users/haijieg/tweets/data/input/test"
      //val outputpath = "/Users/haijieg/tweets/data/output/rt"
	  val spark = new SparkContext(host, "makeRTGraph", System.getenv("SPARK_HOME"),
	      List("/root/spark/TwitterGraphBuilder.jar"))
	  val file = spark.textFile(inputpath)
	  spark.logInfo("Get edges... ")
	  val edgelist = (file flatMap getEdges)
	  edgelist.cache()
	
	  spark.logInfo("Extracted edges: " + (edgelist.count()))
	  spark.logInfo("Get vertices... ")	  
	  // val vertexlist = (file flatMap getVertices)
	  val vertexlist = edgelist flatMap{case (usera,userb) => List(usera, userb)}	  
	  spark.logInfo("Extracted vertices: " + (vertexlist.count()))
	  
	  edgelist.saveAsTextFile(outputpath+"/edges")  
	  vertexlist.saveAsTextFile(outputpath +"/vertices")
	  edgelist.cache()
	  
	  // Raw id to int id map
	  spark.logInfo("Create vidmap... ")	  	  
	  val vidmap = NormalizeVidMap.createVidMap(spark, vertexlist)
	  spark.logInfo("Unique vertices: " + (vidmap.count()))
	  vidmap.saveAsTextFile(outputpath+"/vidmap")
	  
	  // Normalize ids in the edge list
	  spark.logInfo("Create normalized edge... ")	  	  
	  val normalizedEdgeList = NormalizeVidMap.translateEdge(spark, vidmap, edgelist)
	  (normalizedEdgeList map edgeformat) saveAsTextFile(outputpath+"/normalizedEdges")
	  spark.logInfo("Done")
	  sys.exit(0)
  }	
}