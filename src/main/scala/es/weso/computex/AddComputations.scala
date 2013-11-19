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
import scala.collection.JavaConverters
import es.weso.utils.StatsUtils._
import CexUtils._

class AddComputations(arguments: Array[String],
    onError: (Throwable, Scallop) => Nothing
    ) extends ScallopConf(arguments) {

    banner("""| Generate Computations
              | Options:
              |""".stripMargin)
    footer("Enjoy!")
    version("0.1")
    val fileName = opt[String]("file",
                    required=true,
        			descr = "Turtle file")
    val output  = opt[String]("out",
    				descr = "Output file")
    val year = opt[Int]("year", 
                    required=true,
    				descr = "Year of index")
    val version = opt[Boolean]("version", 
    				noshort = true, 
    				descr = "Print version")
    val help 	= opt[Boolean]("help", 
    				noshort = true, 
    				descr = "Show this message")
  
  override protected def onError(e: Throwable) = onError(e, builder)
}


object AddComputations extends App {

 def addNormalize(m:Model) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val iterDataSets = m.listSubjectsWithProperty(rdf_type,qb_DataSet)
   while (iterDataSets.hasNext) {
     val datasetTo = iterDataSets.nextResource()
     if (hasComputationType(m,datasetTo,cex_NormalizeDataSet)) {
       val computation = getComputation(m,datasetTo)
       val datasetFrom = findProperty_asResource(m,computation,cex_dataSet)

       // Iterate for all slices to copy
       val iterSlices = m.listObjectsOfProperty(datasetTo,qb_slice)
       while (iterSlices.hasNext) {
         val sliceTo = iterSlices.nextNode.asResource
         val yearTo = findProperty(m,sliceTo,wf_onto_ref_year)
         
         val iterSlices2 = m.listObjectsOfProperty(datasetFrom,qb_slice)
         while (iterSlices2.hasNext) {
           val sliceFrom = iterSlices2.nextNode.asResource
           val yearFrom = findProperty(m,sliceFrom,wf_onto_ref_year)
           if (yearTo == yearFrom) {
             val indicator = findProperty_asResource(m,sliceFrom,cex_indicator)
             val highLow = findProperty_asResource(m,indicator,cex_highLow)
             val isHigh = highLow == cex_High
             
             // Calculate Mean and SD
             val iterObsMean = m.listObjectsOfProperty(sliceFrom,qb_observation)
             val builder = Seq.newBuilder[Double]
             while (iterObsMean.hasNext) {
               val obs = iterObsMean.nextNode.asResource
               val v = getValue(m,obs)
               if (v.isDefined) builder += v.get
             }
             val seqObs = builder.result
             val (mean,sd) = calculateMeanSD(seqObs)
             
             val iterObs = m.listObjectsOfProperty(sliceFrom,qb_observation)
             while (iterObs.hasNext) {
               val obsFrom = iterObs.nextNode.asResource
               val obsTo = newObs(m)
               newModel.add(sliceTo,qb_observation,obsTo)
               newModel.add(obsTo,rdf_type,qb_Observation)
               copyProperties(obsTo,newModel,
                   Seq(wf_onto_ref_area, wf_onto_ref_year, cex_indicator),obsFrom,m)

               getValue(m,obsFrom) match {
                 case None => {}
                 case Some(value) => {
                    val area = findProperty(m,obsFrom,wf_onto_ref_area)
                    val diff = if (isHigh) value - mean else mean - value 
                    val zScore = diff / sd
                    newModel.add(obsTo,cex_value,literalDouble(zScore))
                 }
               }
               
               newModel.add(obsTo,qb_dataSet,datasetTo)
               newModel.add(obsTo,sdmxConcept_obsStatus,cex_Normalized)
               val newC = newComp(m)
               newModel.add(obsTo,cex_computation,newC)
               newModel.add(newC,rdf_type,cex_Normalize)
               newModel.add(newC,cex_observation,obsFrom)
               newModel.add(newC,cex_slice,sliceFrom)
             }
           }
         }
       }
     }
   }
   newModel
 }

 // Todo: Unfinnished (Check Cluster.sparql query)
 def addCluster(m:Model) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   findDatasetWithComputation(m,cex_ClusterDataSets) match {
     case None => {
       logger.warn("No dataset with computation " + cex_ClusterDataSets)
     }
     case Some((datasetTo,computation)) => {
        val dim = findProperty_asProperty(m,computation,cex_dimension)
        val valueDim = findProperty(m,computation,cex_value)
        val yearDim = valueDim.asLiteral.getInt
          
        val iterSlices = m.listObjectsOfProperty(datasetTo,qb_slice)
        while (iterSlices.hasNext) {
         val sliceTo = iterSlices.nextNode().asResource
         val indicatorTo = findProperty_asResource(m,sliceTo,cex_indicator)

        val iterDataSets = m.listObjectsOfProperty(computation,cex_dataSet)
        while (iterDataSets.hasNext) {
         val datasetFrom = iterDataSets.nextNode().asResource

         val iterSlicesFrom = m.listObjectsOfProperty(datasetFrom,qb_slice)
         while (iterSlicesFrom.hasNext) {
            val sliceFrom = iterSlicesFrom.nextNode.asResource
            val valueDimFrom = findProperty(m,sliceTo,dim)
        
            if (valueDimFrom == valueDim) {
             val indicatorFrom = findProperty_asResource(m,sliceFrom,cex_indicator)
             if (indicatorTo == indicatorFrom) {
               val iterObs = m.listObjectsOfProperty(sliceFrom,qb_observation)
               while (iterObs.hasNext) {
                 val obsFrom = iterObs.nextNode().asResource()
                 if (mustBeClustered(m,obsFrom,yearDim)) { 
                  val obsTo = newObs(m)
                  newModel.add(sliceTo,qb_observation,obsTo)
                  copyProperties(obsTo,newModel,Seq(wf_onto_ref_area, cex_value),obsFrom,m)
                  newModel.add(obsTo,wf_onto_ref_year,valueDim)
                 }
               } 
             }
           } else println("...doesn't match valueDim")
         } 
       }
      }
     }
   }
   newModel
 }


 def mustBeClustered(m: Model, obs: Resource, year: Int) : Boolean = {
   if (isPrimaryObs(m,obs)) getObsYear(m,obs) == year
   else getObsYear(m,obs) == (year - 1)
 }
 
 
 def groupClusters(m:Model) : Model = {
   groupMean(m,cex_GroupClusters,wi_weightSchema_indicatorWeights,cex_Component)
 }


 def groupSubIndex(m:Model) : Model = {
   groupMean(m,cex_GroupSubIndex,wi_weightSchema_componentWeights,cex_SubIndex)
 }

 
 def groupIndex(m:Model) : Model = {
   groupMean(m,cex_GroupIndex,wi_weightSchema_subindexWeights,cex_Index)
 }

 def groupMean(m:Model, compType: Resource, weightSchema: Resource, level: Resource) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   findDatasetWithComputation(m,compType) match {
     case None =>{
       logger.warn("No dataset with computation " + compType)
       newModel
     } 
     case Some((dataset,computation)) => {
       val datasetFrom = findProperty_asResource(m, computation, cex_dataSet)
       val dimension   = findProperty_asResource(m, computation, cex_dimension)

       val table = collectObservations(m, datasetFrom)
       val groupings = collectGroupings(m, level)
       val weights = collectWeights(m, weightSchema)
       val newTable = table.group(groupings,weights)
       saveTable(newModel,m,dataset,newTable,weightSchema)
       newModel
     }
   }
 }

 def saveTable(newModel : Model,oldModel : Model, dataset: Resource, table: TableComputations, weights: Resource) : Unit = {
   val iterSlicesTo = oldModel.listObjectsOfProperty(dataset,qb_slice)
   while (iterSlicesTo.hasNext) {
         val sliceTo = iterSlicesTo.next.asResource
         val indicator = findProperty_asResource(oldModel,sliceTo,cex_indicator)
         val year = findProperty(oldModel,sliceTo,wf_onto_ref_year)
         for (area <- table.getAreas) {
           if (table.contains(indicator,area)) {
        	 val obs = newObs(newModel)
             newModel.add(obs,rdf_type,qb_Observation)
             newModel.add(obs,cex_indicator,indicator)
             newModel.add(obs,wf_onto_ref_area,area)
             newModel.add(obs,wf_onto_ref_year,year)
             addValueComputed(newModel,obs,indicator,area,table,weights)
             newModel.add(sliceTo,qb_observation,obs)
           }
         }
        }
 }

 def collectWeights(m:Model, w: Resource) = {
   val weightsBuilder = Map.newBuilder[Resource,Double]
   val iterWeights = m.listObjectsOfProperty(w,cex_weight)
   while (iterWeights.hasNext) {
      val weightNode = iterWeights.nextNode().asResource()
      val element = findProperty_asResource(m,weightNode,cex_element)
      val weight  = getValue(m,weightNode).get
      weightsBuilder += ((element,weight))
    }
    weightsBuilder.result
 }

 def collectGroupings(m:Model, typeComponent: Resource) = {
   val iterComponents = m.listSubjectsWithProperty(rdf_type,typeComponent)
   val groupingsBuilder = Map.newBuilder[Resource,Set[Resource]]
   while (iterComponents.hasNext) {
     val component = iterComponents.nextResource()
     val iterElements = m.listObjectsOfProperty(component,cex_element)
     val elemsBuilder = Set.newBuilder[Resource]
     while(iterElements.hasNext) {
        val element = iterElements.next.asResource
        elemsBuilder += element
      }
     groupingsBuilder+= ((component,elemsBuilder.result))
    }
   groupingsBuilder.result
 }

 
 def collectObservations(m: Model, dataset: Resource) : TableComputations = {
   val table = TableComputations.newTable
   val iterSlices = m.listObjectsOfProperty(dataset,qb_slice)
   while (iterSlices.hasNext) {
    val slice = iterSlices.next.asResource
    val indicator = findProperty_asResource(m,slice,cex_indicator)
    val iterObs = m.listObjectsOfProperty(slice,qb_observation)
    while (iterObs.hasNext) {
       val obs = iterObs.next.asResource()
       val area = getObsArea(m,obs) 
       getObsValue(m,obs) match {
         case None => {}
         case Some(value) => {
           table.insert(indicator,area,Some(value,Seq()))
         }
       }
      }
   }
   table
 }
 

 def addValueComputed(m: Model, obs: Resource, indicator: Resource, area: Resource, table: TableComputations, weights: Resource) : Unit = {
   m.add(obs,cex_value,literalDouble(table.lookupValue(indicator,area)))
   val comp = newComp(m)
   m.add(obs,cex_computation,comp)
   m.add(comp,rdf_type,cex_GroupMean)
   m.add(comp,cex_weightSchema,weights)

   val group = newResource(m)
   m.add(comp,cex_group,group)
   table.lookupList(indicator,area).foreach {
    p => { val tuple = newResourceNoBlankNode(m, webindex_bnode)
       	   m.add(group,cex_weightMap,tuple)
           m.add(tuple,cex_element,p._1)
           m.add(tuple,cex_value,literalDouble(p._2))
           m.add(tuple,cex_weight,literalDouble(p._3))
    }
   }
 }
 
 def addComputations(m: Model,year:Int) : Model = {
   AddDatasets.addDatasets(m,year)
   val normalize = addNormalize(m)
   println("Normalized: " + normalize.size)
   m.add(normalize)  

   val clustered = addCluster(m)
   println("Clustered: " + clustered.size)
   m.add(clustered)
   
   val groups = groupClusters(m)
   println("Groups: " + groups.size)
   m.add(groups) 

   val subindexes = groupSubIndex(m)
   println("Subindexes: " + subindexes.size)
   m.add(subindexes) 

   val index = groupIndex(m)	
   println("Index: " + index.size)
   m.add(index) 

   AddRanking.addRankings(m) 
   
   m
 } 

 override def main(args: Array[String]) {

  val logger 		= LoggerFactory.getLogger("Application")
  
  val conf 			= ConfigFactory.load()
  
  val opts 	= new AddDatasetsOpts(args,onError)
  try {
   val model = ModelFactory.createDefaultModel
   val inputStream = FileManager.get.open(opts.fileName())
   model.read(inputStream,"","TURTLE")
   val newModel = addComputations(model, opts.year())
   if (opts.output.get == None) newModel.write(System.out,"TURTLE")
   else {
     val fileOutput = opts.output()
     newModel.write(new FileOutputStream(fileOutput),"TURTLE")
   }
  } catch {
    case e: Exception => println("\nException:\n" + e.getLocalizedMessage())
  }
 }

  private def onError(e: Throwable, scallop: Scallop) = e match {
    case Help(s) =>
      println("Help: " + s)
      scallop.printHelp
      sys.exit(0)
    case _ =>
      println("Error: %s".format(e.getMessage))
      scallop.printHelp
      sys.exit(1)
  }
  
  
} 
