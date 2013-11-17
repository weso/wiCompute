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
import scala.collection.mutable.ArrayBuffer
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
import scala.collection.JavaConversions._
import es.weso.utils.StatsUtils._

object CexUtils {
  
 def hasComputationType(m:Model, r: Resource, t: Resource) : Boolean = {
   if (hasProperty(m,r,cex_computation)) {
    val c = findProperty_asResource(m,r,cex_computation)
    val cType = findProperty_asResource(m,c,rdf_type)
    cType.asResource == t
   } else false
   
 }

 def getComputation(m:Model,r:Resource) : Resource = {
   if (hasProperty(m,r,cex_computation)) {
    findProperty_asResource(m,r,cex_computation)
   } else 
    throw new Exception("Cannot find computation for resource " + r)
 }

 def getValue(m:Model,obs:Resource) : Option[Double] = {
   if (hasProperty(m,obs,cex_value)) {
     Some(findProperty(m,obs,cex_value).asLiteral.getDouble)
   } else 
     None
 }

  def findDatasetWithComputation(m:Model, compType : Resource) : 
	  		Option[(Resource,Resource)] = {
   val iterDatasets = m.listSubjectsWithProperty(rdf_type,qb_DataSet)
   while (iterDatasets.hasNext) {
     val dataset = iterDatasets.next
     if (hasComputationType(m,dataset,compType)) {
       return Some(dataset,findProperty_asResource(m,dataset,cex_computation))       
     }
   }
   None
 }
  
 def getIndicators(m:Model): Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,cex_Indicator).toList
 
 def getComponents(m:Model): Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,cex_Component).toList

 def getSubindexes(m:Model): Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,cex_SubIndex).toList

 def getDatasets(m:Model):Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,qb_DataSet).toList
 
 def getCountries(m:Model):Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,wf_onto_Country).toList

 def findDataset(m: Model, datasetName: String) : Option[Resource] = {
   getDatasets(m).find(d => d.getLocalName == datasetName)
 }

 def getProperty(m: Model, r: Resource, p: Property) : Option[Resource] = {
   if (hasProperty(m,r,p)) Some(findProperty_asResource(m,r,p))
   else None
 }
 
 def getDataset(m: Model, r: Resource) : Option[Resource] = {
   getProperty(m,r,qb_dataSet)
 }

 def findIndicator(m: Model, indicatorName: String) : Option[Resource] = {
   getIndicators(m).find(i => i.getLocalName == indicatorName)
 }
 
 def findComponent(m: Model, componentName: String) : Option[Resource] = {
   getComponents(m).find(i => i.getLocalName == componentName)
 }

 def findSubIndex(m: Model, subindexName: String) : Option[Resource] = {
   getSubindexes(m).find(i => i.getLocalName == subindexName)
 }

 def findCountry(m: Model, countryName: String) : Option[Resource] = {
   getCountries(m).find(c => c.getLocalName == countryName)
 }
 
 def getObs(m:Model, indicator: Resource, year: Int, country: Resource, cond: Resource => Boolean) : Seq[Resource] = {
     val builder = Seq.newBuilder[Resource]
     val iterObs = m.listSubjectsWithProperty(cex_indicator,indicator)
     while (iterObs.hasNext) {
       val obs = iterObs.nextResource()
       if (obs.hasProperty(rdf_type,qb_Observation) &&
           obs.hasProperty(wf_onto_ref_area,country) &&
           obs.hasProperty(wf_onto_ref_year,literalInteger(year)) &&
           cond(obs)
           ) 
       builder += obs
     }
    builder.result
 }

  def findObs(m:Model, indicator: Resource, year: Int, country: Resource, cond: Resource => Boolean) : Option[Resource] = {
   val lsObs = getObs(m,indicator,year,country,cond) 
   lsObs.length match {
     case 1 => Some(lsObs.head)
     case 0 => None
     case _ => throw new Exception("More than one observation with indicator " + indicator + ", year: " + year + ", country " + country + " satisfies condition")
   }  
 }

 def findObs(m:Model, indicatorName: String, year: Int, countryCode: String, cond: Resource => Boolean) : Option[Resource] = {
   for (i <- findIndicator(m,indicatorName);
        c <- findCountry(m,countryCode);
        val lsObs = getObs(m,i,year,c,cond) ;
        if lsObs.length == 1 ) 
   yield lsObs.head
 }

 def findValue(m:Model, indicator: Resource, year: Int, country: Resource, cond: Resource => Boolean) : Option[Double] = {
   for ( o <- findObs(m,indicator,year,country,cond);
         v <- getValue(m,o))
   yield v
 }

 def findValue(m:Model, indicatorName: String, year: Int, countryCode: String, cond: Resource => Boolean) : Option[Double] = {
   for ( o <- findObs(m,indicatorName,year,countryCode,cond);
         v <- getValue(m,o))
   yield v
 }

 def findValueInDataset(m:Model, indicatorName: String, year: Int, countryCode: String, datasetName: String) : Option[Double] = {
   val cond : Resource => Boolean = r => 
     getDataset(m,r) match {
       case None => false
       case Some(d) => d.getLocalName == datasetName
    }
   findValue(m,indicatorName,year,countryCode,cond)
 }

 def findValueCompType(m:Model, indicatorName: String, year: Int, countryCode: String, compType: Resource) : Option[Double] = {
   val cond : Resource => Boolean = r => hasComputationType(m,r,compType)
   findValue(m,indicatorName,year,countryCode,cond)
 }
   
 def findValueCompType(m:Model, indicator: Resource, year: Int, country: Resource, compType: Resource) : Option[Double] = {
   val cond : Resource => Boolean = r => hasComputationType(m,r,compType) 
   findValue(m,indicator,year,country,cond)
 }
 
  def findProperty_asProperty(m:Model, r:Resource, p:Property): Property = {
   val vr = findProperty_asResource(m,r,p)
   if (vr.isURIResource()) {
     m.getProperty(vr.getURI)
   } else
     throw new Exception("findProperty_asProperty: value of property " + p + 
                         " for resource " + r + " is " + vr + ", but should be an URI")
 }
 def copyProperties(to:Resource, modelTo: Model, ps:Seq[Property],from:Resource,modelFrom:Model): Unit = {
   ps.foreach(p => copyProperty(to,modelTo,p,from,modelFrom))
 }

 def copyProperty(to: Resource, modelTo: Model,p: Property, from: Resource, modelFrom: Model): Unit = {
   if (hasProperty(modelFrom,from,p)) {
     val value = findProperty(modelFrom,from,p)
     modelTo.add(to,p,value)
   }
 }
 
 def getObsValue(m: Model, obs: Resource) : Double = {
   val value = findProperty(m,obs,cex_value)
   value.asLiteral.getDouble()
 }
 
 def getObsArea(m: Model, obs: Resource) : Resource = {
   findProperty_asResource(m,obs,wf_onto_ref_area)
 }

 def isPrimary(m:Model, indicator: Resource) : Boolean = {
   m.contains(indicator,rdf_type,wf_onto_PrimaryIndicator)
 }
 
  def isPrimary(m:Model, indicatorName: String) : Boolean = {
   findIndicator(m, indicatorName) match {
     case None => throw new Exception("isPrimary: " + indicatorName + " is not an indicator name")
     case Some(indicator) =>
       m.contains(indicator,rdf_type,wf_onto_PrimaryIndicator)
   }
 }

}