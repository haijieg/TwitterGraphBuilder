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
    println ("usage: FollowerJoinLda <hostname> <sparkhome> <inputpath> <outputpath>")
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
      val spark = new SparkContext(host, "FollowerJoinLda", sparkhome,
          List("target/deps.jar", "target/scala-2.9.2/twittergraphbuilder_2.9.2-0.0.1.jar"))  

      /* Get user topic feature from the result of cgs_lda */ 
      val usertopic = spark.textFile(inputpath + "/lda/users").map(
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
	  val usermap = spark.broadcast(((uniqusers.collect()) zip (0 to numusers-1)) toMap)
	  
	  System.out.println("Join user id map and tweets count... ")	        
	  val tweetscountmap = 
	      (tweetscount.
	          filter{case (name, count) => usermap.value.contains(name)}.
	          map{case (name, count) => (usermap.value(name) + numwords, count)})
	  
	  val tweetscount_bc = spark.broadcast(tweetscountmap.collectAsMap());
	  System.out.println("Join tweets count map and user topic... ")	        	       
      val userfeatures = usertopic.map{
        case (id, topics) => {
          id + "\t" + tweetscount_bc.value.getOrElse(id, 1) + "\t" + topics.foldLeft("")((x,y) => x + "\t" + y)
        }
      }            
      userfeatures.saveAsTextFile(outputpath)     
  }	
}

