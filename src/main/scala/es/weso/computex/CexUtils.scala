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
import scala.collection.SortedSet

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

 def getYear(m:Model,r:Resource) : Int = {
   findYear(m,r) match {
     case Some(y) => y
     case None => throw new Exception("Cannot find year of " + r)
   }
 }

 def getValue(m:Model,r:Resource) : Double = {
   findValue(m,r) match {
     case Some(v) => v
     case None => throw new Exception("Cannot find value of " + r)
   }
 }

 def findValue(m:Model,obs:Resource) : Option[Double] = {
   if (hasProperty(m,obs,cex_value)) {
     Some(findProperty(m,obs,cex_value).asLiteral.getDouble)
   } else 
     None
 }

 def findComputationYear(m:Model, comp: Resource): Option[Int] = {
   if (hasProperty(m,comp,cex_filterDimension) && 
       findProperty_asResource(m,comp,cex_filterDimension) == wf_onto_ref_year) {
     if (hasProperty(m,comp,cex_filterValue)) {
       Some(findProperty(m,comp,cex_filterValue).asLiteral.getInt)
     } else
       None
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

  /**
   * Finds a dataset with a given computation and year
   */
  def findDatasetWithComputationYear(m:Model, compType : Resource, year: Int) : 
	  		Option[(Resource,Resource)] = {
   val iterDatasets = m.listSubjectsWithProperty(rdf_type,qb_DataSet)
   var pairFound : Option[(Resource,Resource)]= None
   while (iterDatasets.hasNext && pairFound == None) {
     val dataset = iterDatasets.next
     if (hasComputationType(m,dataset,compType)) {
       val computation = findProperty_asResource(m,dataset,cex_computation)
       findComputationYear(m,computation) match {
         case Some(y) => if (y == year) { pairFound = Some(dataset,computation) }
         case None => println("Computation " + computation + " of type " + compType + " but no year")
       }
     }
   }
   pairFound
 }

 /**
  * Returns the list of datasets 
  */
 def getDatasets(m:Model):Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,qb_DataSet).toList
 
 /**
  *   Returns the slices of a dataset
  */  
 def getDatasetSlices(m:Model,dataset: Resource): Seq[Resource] = {
   m.listObjectsOfProperty(dataset,qb_slice).map(_.asResource).toList
 }

 /**
  * Returns a the list of pairs (slice,indicator) of a dataset
  */
 def getDatasetSlicesIndicators(m:Model,dataset: Resource): Seq[(Resource,Resource)] = {
   val ls = m.listObjectsOfProperty(dataset,qb_slice).map(_.asResource).toList
   ls.map(slice => { 
     val indicator = findProperty_asResource(m,slice,cex_indicator)
     (slice,indicator)
   })
 }

 def getComputationDatasets(m:Model,comp: Resource): Seq[Resource] = {
   m.listObjectsOfProperty(comp,cex_dataSet).map(_.asResource).toList
 }
 
 /**
  * Returns the list of observations of a slice
  */
 def getObservationsSlice(m:Model, slice: Resource): Seq[Resource] = {
   m.listObjectsOfProperty(slice,qb_observation).map(_.asResource).toList
 }

   
 def getIndicators(m:Model): Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,cex_Indicator).toList
 
 def getPrimaryIndicators(m:Model): Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,wf_onto_PrimaryIndicator).toList

 def getSecondaryIndicators(m:Model): Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,wf_onto_SecondaryIndicator).toList

 def getComponents(m:Model): Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,cex_Component).toList

 def getSubindexes(m:Model): Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,cex_SubIndex).toList

 def getIndexes(m:Model): Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,cex_Index).toList
   
 def getYears(m:Model):SortedSet[Int] = {
   val slices : Seq[Resource] = m.listSubjectsWithProperty(rdf_type,qb_Slice).toList
   setOfDefined(slices.map(s => findYear(m,s)))
 }
 
 def setOfDefined(s: Seq[Option[Int]]): SortedSet[Int] = {
   SortedSet(s.filter(_.isDefined).map(_.get): _*)
 }
 
 def getSlices(m:Model, dataset: Resource) : Seq[Resource] = {
   m.listStatements(dataset,qb_slice,null : RDFNode).map(st => st.getObject.asResource).toList
 }

 def findYear(m:Model, r:Resource) : Option[Int] = {
   for (y <- getProperty(m,r,wf_onto_ref_year); if y.isLiteral) 
   yield (y.asLiteral.getInt)
 }

 def getCountries(m:Model):Seq[Resource] = 
   m.listSubjectsWithProperty(rdf_type,wf_onto_Country).toList

 def findDataset(m: Model, datasetName: String) : Option[Resource] = {
   getDatasets(m).find(d => d.getLocalName == datasetName)
 }

 def getProperty(m: Model, r: Resource, p: Property) : Option[RDFNode] = {
   if (hasProperty(m,r,p)) Some(findProperty(m,r,p))
   else None
 }
 
 def getProperty_asResource(m: Model, r: Resource, p: Property) : Option[Resource] = {
   if (hasProperty(m,r,p)) Some(findProperty_asResource(m,r,p))
   else None
 }

 def getDataset(m: Model, r: Resource) : Option[Resource] = {
   getProperty_asResource(m,r,qb_dataSet)
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

 def lookupValue(m:Model, indicator: Resource, year: Int, country: Resource, cond: Resource => Boolean) : Option[Double] = {
   for ( o <- findObs(m,indicator,year,country,cond);
         v <- findValue(m,o))
   yield v
 }

 def lookupValue(m:Model, indicatorName: String, year: Int, countryCode: String, cond: Resource => Boolean) : Option[Double] = {
   for ( o <- findObs(m,indicatorName,year,countryCode,cond);
         v <- findValue(m,o))
   yield v
 }

 def findValueInDataset(m:Model, indicatorName: String, year: Int, countryCode: String, datasetName: String) : Option[Double] = {
   val cond : Resource => Boolean = r => 
     getDataset(m,r) match {
       case None => false
       case Some(d) => d.getLocalName == datasetName
    }
   lookupValue(m,indicatorName,year,countryCode,cond)
 }

 def findValueCompType(m:Model, indicatorName: String, year: Int, countryCode: String, compType: Resource) : Option[Double] = {
   val cond : Resource => Boolean = r => hasComputationType(m,r,compType)
   lookupValue(m,indicatorName,year,countryCode,cond)
 }
   
 def findValueCompType(m:Model, indicator: Resource, year: Int, country: Resource, compType: Resource) : Option[Double] = {
   val cond : Resource => Boolean = r => hasComputationType(m,r,compType) 
   lookupValue(m,indicator,year,country,cond)
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
 
 def getObsValue(m: Model, obs: Resource) : Option[Double] = {
   if (hasProperty(m,obs,cex_value)) {
     val value = findProperty(m,obs,cex_value)
     Some(value.asLiteral.getDouble())
   } else 
     None
 }
 
 def getObsArea(m: Model, obs: Resource) : Resource = {
   findProperty_asResource(m,obs,wf_onto_ref_area)
 }

 def getObsYear(m: Model, obs: Resource) : Int = {
   findProperty(m,obs,wf_onto_ref_year).asLiteral.getInt
 }

 def getIndicator(m: Model, r: Resource) : Resource = {
   if (hasProperty(m,r,cex_indicator)) 
     findProperty_asResource(m,r,cex_indicator)
   else 
     throw 
      new Exception("getIndicator:" + r + " does not have value for " + cex_indicator)
 }

 def isPrimary(m:Model, indicator: Resource) : Boolean = {
   val r = m.contains(indicator,rdf_type,wf_onto_PrimaryIndicator)
   r
 }
 
  def isPrimary(m:Model, indicatorName: String) : Boolean = {
   findIndicator(m, indicatorName) match {
     case None => throw new Exception("isPrimary: " + indicatorName + " is not an indicator name")
     case Some(indicator) =>
       m.contains(indicator,rdf_type,wf_onto_PrimaryIndicator)
   }
 }

 def isPrimaryResource(m:Model, r: Resource) : Boolean = {
   val indicator = getIndicator(m,r)
   isPrimary(m,indicator)
 }


 // TODO: refactor this ugly code...
 def isPrimaryDataset(m : Model, d: Resource) : Boolean = {
   var foundPrimary = false
   if (hasProperty(m,d,qb_slice)) {
     val iterSlices = m.listObjectsOfProperty(d,qb_slice)
     while (iterSlices.hasNext) {
       val slice = iterSlices.next.asResource
       if (isPrimaryResource(m,slice)) foundPrimary = true
     }
   } 
   foundPrimary
 }


 def newResource(m: Model) = newResourceNoBlankNode(m, webindex_bnode)
 def newObs(m: Model, year:Int) = newResourceNoBlankNode(m, wi_obsComputed + "_" + year + "_")
 def newComp(m: Model) = newResourceNoBlankNode(m, wi_compComputed)
 def newComp(m: Model,year:Int) = newResourceNoBlankNode(m, wi_compComputed+"_"+year+"_")
 def newDataset(m: Model) = newResourceNoBlankNode(m, wi_datasetComputed)
 def newDataset(m: Model, year:Int) = newResourceNoBlankNode(m, wi_datasetComputed + "_" + year+"_")
 def newSlice(m: Model, year:Int) = newResourceNoBlankNode(m, wi_sliceComputed+ "_" + year+"_")
 def newSlice(m: Model) = newResourceNoBlankNode(m, wi_sliceComputed)

 def mkSlice(m: Model, s: String, year: Int) : Resource = 
   m.createResource(wi_slice + s + "_" + year)

 def mkSlice(m:Model, r: Resource,year:Int) : Resource = 
   mkSlice(m,r.getLocalName,year)
  
  def mkClusterIndicators(m: Model, year: Int): Resource = {
   m.createResource(wi_dataset + "ClusterIndicators_" + year)
 }
 
 def mkClustersGrouped(m: Model, year: Int): Resource = {
   m.createResource(wi_dataset + "ClustersGrouped_" + year)
 }
 
 def mkSubIndexGrouped(m: Model,year: Int): Resource = {
   m.createResource(wi_dataset + "SubIndexGrouped_" + year)
 }

 def mkComposite(m: Model,year: Int): Resource = {
   m.createResource(wi_dataset + "Composite_" + year)
 }

 def mkRankings(m: Model, year: Int): Resource = {
   m.createResource(wi_dataset + "Rankings_" + year)
 }

 def mkRanking(m: Model, s: String, year:Int) : Resource  = 
   m.createResource(wi_ranking + s + "_" + year)

 def mkRanking(m: Model, r: Resource, year:Int): Resource = 
   mkRanking(m,r.getLocalName,year)

 def mkScores(m: Model,year: Int): Resource = {
   m.createResource(wi_dataset + "Scores_" + year)
 }

 def mkScore(m: Model, s: String, year:Int) : Resource  = 
   m.createResource(wi_score + s + "_" + year)

 def mkScore(m: Model, r: Resource, year:Int): Resource = 
   mkScore(m,r.getLocalName,year)

}