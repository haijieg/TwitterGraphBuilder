package graphbuilder.twitter
import spark.SparkContext
import SparkContext._
import scala.collection.mutable
import spark.RDD
import scala.collection.mutable.HashMap

object NormalizeVidMap {
  def uniqIds(sc: SparkContext, vertexlist: spark.RDD[String]) : spark.RDD[String] = {
    (vertexlist.map (w => (w, 1))) reduceByKey(_+_, 32) map {case (x, _) => x} 
  }
  
  def translateEdge(sc: SparkContext, vidmap: spark.RDD[(String, Int)], edgelist: spark.RDD[(String, String)]) :
  	spark.RDD[(Int, Int)] = {
    val transSource = vidmap.join(edgelist, 32).map { case (source, (sourceid, target)) => (target, sourceid)}
    val transTarget = transSource.join(vidmap, 32).map { case (target, (targetid, sourceid)) => (sourceid, targetid)}
    transTarget 
  }
  
    def translateEdgeWithData[T](sc: SparkContext, vidmap: spark.RDD[(String, Int)],
        edgelist: spark.RDD[((String), (String, T))]) : spark.RDD[(Int, Int, T)] = {
    val transSource = vidmap.join(edgelist, 32).map { case (source, (sourceid, (target, data))) => (target, (sourceid, data))}
    val transTarget = vidmap.join(transSource, 32).map { case (target, (targetid, (sourceid, data))) => (sourceid, targetid, data)}
    transTarget 
  }
}