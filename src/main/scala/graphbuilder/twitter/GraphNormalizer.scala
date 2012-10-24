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
object GraphNormalizer {

  def edgeformat (e : (Int, Int, Int)): String = {
    e._1 + "\t" + e._2 + "\t" + e._3
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
	
	  /* Build Rawid to Normalized Id map */
	  System.out.println("Get user/word list... ")
	  val uniqusers = spark.textFile(inputpath + "/vidmap/users")
	  val uniqwords = spark.textFile(inputpath + "/vidmap/words")	  
	  val numwords = uniqwords.count().toInt
	  val numusers = uniqusers.count().toInt
	  System.out.println("Unique users: " + numwords)
	  System.out.println("Unique words: " + numusers)	  
	  
	  
	  /* Normalize edges */
	  System.out.println("Create user/word id map... ")	  
	  val wordmap = spark.broadcast(((uniqwords.collect()) zip (0 to numwords-1)) toMap)
	  val usermap = spark.broadcast(((uniqusers.collect()) zip (0 to numusers-1)) toMap)
	 
	   /* Get Edges */
	  System.out.println("Get edges... ")
	  val edgelist = spark.textFile(inputpath +"/edges").
	  					map { w => {val sp = w.split("\t"); (sp(0), sp(1), sp(2).toInt)}}      	  
	 
      System.out.println("Create normalized edge... ")	  
	  val normalizedEdgeList = edgelist map {
	    case ((user, word, count)) => (usermap.value(user) + numwords, wordmap.value(word), count)
	  }
	  (normalizedEdgeList map edgeformat) saveAsTextFile(outputpath +"/normalizedEdges")
	  	 
	  	  
	  /*
	  /* Normalize edges */
	  System.out.println("Create user/word id map... ")	  
	  val wordmap = uniqwords.cartesian(spark.parallelize(0 to numwords-1)).partitionBy((new HashPartitioner(8)))
	  val usermap = uniqusers.cartesian(spark.parallelize(numwords to numwords+numusers-1)).partitionBy((new HashPartitioner(8)))	  	  
	  
	  
	  /* Get Edges */
	  System.out.println("Get edges... ")
	  val edgelist = spark.textFile(inputpath +"/edges").
	  					map { w => {val sp = w.split("\t"); ((sp(0)), (sp(1), sp(2).toInt))}}      	  
	  /* Normalize edges */
	  System.out.println("Create normalized edge... ")
	  val normalizedEdgeList = NormalizeVidMap.translateEdgeWithData(spark, usermap, wordmap, edgelist)
	  (normalizedEdgeList map edgeformat) saveAsTextFile(outputpath +"/normalizedEdges")
	  */
	  	 
	  System.out.println("Done")  
	  sys.exit(0)
  }	
}