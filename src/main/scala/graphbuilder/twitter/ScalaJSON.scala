package graphbuilder.twitter
import net.minidev.json._
import scala.collection.JavaConversions._

class ScalaJSON (o: java.lang.Object) {
  override def toString : String = o.toString
  
  def toInt : Int = o match {
    case i : Integer => i
    case _ => throw new JSONException
  }
  
  def toDouble : Double = o match {
    case d: java.lang.Double => d
    case f: java.lang.Float => f.toDouble
    case _ => throw new JSONException
  }
  
  def get(key: String): ScalaJSON = o match {
    case m: JSONObject => {
     if (m.contains(key)) 
       new ScalaJSON(m.get(key))
     else
       throw new JSONException
    }
    case _ => throw new JSONException
  }
  
  def get(idx: Int) : ScalaJSON = o match {
    case m: JSONArray => new ScalaJSON(m.get(idx))
    case _ => throw new JSONException
  }
}

case class JSONException extends Exception