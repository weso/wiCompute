package es.weso.computex

import org.rogach.scallop.Scallop
import com.typesafe.config.ConfigFactory
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help
import org.slf4j.LoggerFactory
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.rdf.model.Model
import scala.io.Source
import es.weso.utils.JenaUtils._
import com.hp.hpl.jena.query.ResultSet
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.FileOutputStream
import com.hp.hpl.jena.rdf.model.SimpleSelector
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.Literal
import com.hp.hpl.jena.rdf.model.Property
import PREFIXES._
import scala.collection.JavaConverters
import es.weso.utils.StatsUtils._
import scala.collection.immutable.HashMap


case class TableComputations() {
    
val table = scala.collection.mutable.HashMap[(Resource,Resource),Option[(Double,Seq[(Resource,Double,Double)])]]()
val areas = scala.collection.mutable.Set[Resource]() 

def insert(i: Resource, a: Resource, v: Double, vs: Seq[(Resource,Double,Double)]) : Unit = {
   insert(i,a,Some(v,vs))
  }

def insert(i: Resource, a: Resource, v: Option[(Double,Seq[(Resource,Double,Double)])]) : Unit = {
  table.put((i,a), v)
  areas+= a 
}

def contains(i: Resource, a: Resource) : Boolean = {
  if (table.contains((i,a))) lookup(i,a) != None
  else false
}

def lookup(i : Resource, a: Resource) : Option[(Double,Seq[(Resource,Double,Double)])] = {
  if (table.contains((i,a))) table((i,a))
  else None
}

def lookupValue(i : Resource, a: Resource) : Double = {
  table((i,a)).get._1
}

def lookupList(i : Resource, a: Resource) : Seq[(Resource,Double,Double)]= {
  table((i,a)).get._2
}

def getAreas: Set[Resource] = {
  areas.toSet
}

def group(groupings: Map[Resource,Set[Resource]],
		  weights:Map[Resource,Double],
		  doMean : Boolean
         ): TableComputations = {
  try {
   val areas = getAreas
   val result = TableComputations.newTable

   for (c <- groupings.keys ; a <- areas) {
     val ps = (for (i <- groupings(c) ; if (lookup(i,a) != None)) 
    	 	   yield (i, lookupValue(i,a), weights(i)) ).toSeq
     weightedAvg(ps,doMean) match {
       case None => result.insert(c,a,None)
       case Some(v) => result.insert(c,a,Some((v,ps)))
     }
   }
   result
  } catch {
    case e: Exception => {
      println("Exception grouping: " + e)
      TableComputations.newTable
    }
   }
 }
  

def weightedAvg(vs : Seq[(Resource,Double,Double)], doMean : Boolean ) : Option[Double] = {
   if (vs.isEmpty) None
   else {
     val sum = vs.foldLeft(0.0)((r,p)=> p._2 * p._3 + r)
     if (doMean) {
       val countNonCero = vs.filter(t => t._3 != 0.0).length
       Some(sum / countNonCero.toDouble)
     }
     else 
       Some(sum)
   }
 }

 def getIndicators : Iterable[Resource] = 
   table.keys.map(p => p._1) 

 def show: Unit = {
   print("         ")
   getIndicators.foreach(i => print(i + " "))
   println
   getAreas.foreach(a => {
     print(a + " " )
     getIndicators.foreach(i => print(lookup(i,a) + "     "))
     println
   })
 }

 def showInfo: Unit = {
   println("Table. Areas: " + getAreas.size + ", Indicators: " + getIndicators.size)
   println("Areas: " + getAreas)
   println("Indicators: " + getIndicators)
 }

 def empty : Unit = table.clear

}

object TableComputations {
  
  
  def newTable : TableComputations = new TableComputations()
}