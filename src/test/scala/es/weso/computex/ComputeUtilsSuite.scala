package es.weso.computex

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import es.weso.utils.JenaUtils._
import es.weso.utils.JenaUtils
import CexUtils._
import PREFIXES._
import scala.io.Source
import scala.util.parsing.json._
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.Resource

trait ComputeUtilsSuite extends FunSpec 
	with ShouldMatchers {

  val epsilon: Float = (0.01).toFloat

  def matchDoublesDataset(m:Model,i:String, y: Int, c:String, d: String,v: Double) = {
      findValueInDataset(m,i,y,c,d) match {
        case None => fail("Not found obs (" + i + "," + y + "," + c + "," + d + ")")
        case Some(found) => matchDoubles(found,v)
      }
    }
    
  def matchDoublesCompType(m:Model,i:Resource, y: Int, c:Resource, ct: Resource, v:Double) = {
      findValueCompType(m,i,y,c,ct) match {
        case None => fail("Not found obs (" + i + "," + y + "," + c + "," + ct.toString + ")")
        case Some(found) => matchDoubles(found,v)
      }
    }

  def matchDoublesCompType(m:Model,i:String, y: Int, c:String, ct: Resource, v:Double) = {
      findValueCompType(m,i,y,c,ct) match {
        case None => fail("Not found obs (" + i + "," + y + "," + c + "," + ct.toString + ")")
        case Some(found) => matchDoubles(found,v)
      }
    }

  def matchDoubles(d1: Double, d2: Double) = {
      d1.toFloat should be(d2.toFloat plusOrMinus epsilon)
    }
  
    def getDouble(v : Any) : Option[Double] = {
    v match {
      case d : Double => Some(d)
      case _ => None
    } 
  }
  
  def getTableValues(name: String): Map[String,Option[Double]] = {
   val fileName = "examples/" + name + ".json"
   info("...trying to read " + fileName)
   val contents = Source.fromFile(fileName).getLines.mkString
   info("File with values parsed: " + fileName)
   JSON.parseFull(contents) match {
     case Some(json) => {
    	 val mapjson:Map[String,List[Any]] = json.asInstanceOf[Map[String,List[Any]]]
    	 val indicators : List[Any] = mapjson.get("indicators").get
    	 val values : List[Any] = mapjson.get("values").get
    	 val pairs = indicators.map(_.toString) zip values
    	 val map : Map[String,Option[Double]] = pairs.map(t => (t._1, getDouble(t._2))).toMap
    	 map  
     }
     case None => fail("Cannot parse file " + fileName)
   }
  }

}