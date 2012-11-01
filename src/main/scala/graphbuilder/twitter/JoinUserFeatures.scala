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
object JoinUserFeatures {
   def usage() {
    println ("usage: JoinUserFeature <hostname> <sparkhome> <...daily.10k/> <.../daily.10k/userfeatures>")
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
      val spark = new SparkContext(host, "JoinUserFeature", sparkhome,
          List("target/deps.jar", "target/scala-2.9.2/twittergraphbuilder_2.9.2-0.0.1.jar"))  

      /* Get user topic feature from the result of cgs_lda */ 
      val usertopic = spark.textFile(inputpath + "/lda/userfeature").map(
          w => {
            val sp = w.split("\t")
            (sp.head.toInt, sp.tail map (x =>x toInt))
          })
      System.out.println("Loaded " + (usertopic count) + " user topic records");    
      
      /* Get user tweets count feature */       
      val tweetscount = spark.textFile(inputpath + "/usertweetscount").map(
          w => {
            val sp = w.split("\t")
            (sp(0), sp(1).toInt)
          })
      System.out.println("Loaded " + (tweetscount count) + " user tweetscount records");
    
      System.out.println("Create user id map... ")	  
      val uniqusers = spark.textFile(inputpath + "/lda/vidmap/userlist")
	  val uniqwords = spark.textFile(inputpath + "/lda/vidmap/wordlist")	  
	  val numwords = uniqwords.count().toInt
	  val numusers = uniqusers.count().toInt	  
	  
	  System.out.println("Broadcast usermap... ")	        
	  val usermap = spark.broadcast(((numwords to numusers+numwords-1) zip (uniqusers.collect())) toMap)
	  System.out.println("Broadcast tweetscountmap... ")	        	  
	  val tweetscountmap = spark.broadcast(tweetscount.collectAsMap()) 
	          	  
	  System.out.println("Join user id map and tweets count... ")
	  val userfeatures = usertopic.map {case (id, features) => {
	    val username = usermap.value(id)
	    val numtweets = tweetscountmap.value.getOrElse(username, 1)
	    (username, numtweets, features)
	  }}
	  
	  
	  userfeatures.map {
	    case (name, count, features) => 
	      name + "\t" + count + "\t" + features.foldLeft("")((x,y) => x + "\t" + y)
	      }.saveAsTextFile(outputpath)
  }	
}

