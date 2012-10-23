package graphbuilder.twitter.test
import spark.SparkContext

object BroadcastTest {

  def main(args: Array[String]) {
	  
      val host = args(0)
     
      val spark = new SparkContext(host, "makeLDGraph", System.getenv("SPARK_HOME"),
          List("target/deps.jar", "target/scala-2.9.2/twittergraphbuilder_2.9.2-0.0.1.jar"))
	  
      val map = spark.broadcast(Map(1->"one", 2->"two", 3->"three"));
      val list = spark.parallelize(List(1,2,3))
      
      val out = list.map { w => map.value(w)}
      out.foreach(println)
	  sys.exit(0)
  }	
}