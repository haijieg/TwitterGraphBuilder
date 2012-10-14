package graphbuilder.twitter
import spark.SparkContext
import SparkContext._
import scala.collection.mutable
import spark.RDD
import scala.collection.mutable.HashMap

object NormalizeVidMap {
  def createVidMap(sc: SparkContext, vertexlist: spark.RDD[String]) : spark.RDD[(String, Int)]= {
    val rawids = uniqIds(sc, vertexlist)
    val num_vertices = rawids.count().toInt
    /*val newids = sc.parallelize(0 to num_vertices-1)
    rawids.cartesian(newids)*/
    sc.parallelize(rawids.collect().zip((0 to num_vertices -1)))
  }
  
  def uniqIds(sc: SparkContext, vertexlist: spark.RDD[String]) : spark.RDD[String] = {
    ((vertexlist.map (w => (w, ()))).groupByKey(256)).map {case (x, _) => x} 
  }
  
  def translateEdge(sc: SparkContext, vidmap: spark.RDD[(String, Int)], edgelist: spark.RDD[(String, String)]) :
  	spark.RDD[(Int, Int)] = {
    val transSource = vidmap.join(edgelist, 256).map { case (source, (sourceid, target)) => (target, sourceid)}
    val transTarget = transSource.join(vidmap, 256).map { case (target, (targetid, sourceid)) => (sourceid, targetid)}
    transTarget 
  }
  
    def translateEdgeWithData[T](sc: SparkContext, vidmap: spark.RDD[(String, Int)],
        edgelist: spark.RDD[((String), (String, T))]) : spark.RDD[(Int, Int, T)] = {
    val transSource = vidmap.join(edgelist, 256).map { case (source, (sourceid, (target, data))) => (target, (sourceid, data))}
    val transTarget = vidmap.join(transSource, 256).map { case (target, (targetid, (sourceid, data))) => (sourceid, targetid, data)}
    transTarget 
  }
}