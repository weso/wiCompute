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
import CexUtils._

class AddComputationsOpts(arguments: Array[String],
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
    val year  	= opt[Int]("year",
                    required = true,
    				descr = "Year of current WebIndex")
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
               val v = findValue(m,obs)
               if (v.isDefined) builder += v.get
             }
             val seqObs = builder.result
             val (mean,sd) = calculateMeanSD(seqObs)
             
             val iterObs = m.listObjectsOfProperty(sliceFrom,qb_observation)
             while (iterObs.hasNext) {
               val obsFrom = iterObs.nextNode.asResource
               val obsTo = newObs(m,yearTo.asLiteral.getInt)
               newModel.add(sliceTo,qb_observation,obsTo)
               newModel.add(obsTo,rdf_type,qb_Observation)
               copyProperties(obsTo,newModel,
                   Seq(wf_onto_ref_area, wf_onto_ref_year, cex_indicator),obsFrom,m)

               findValue(m,obsFrom) match {
                 case None => {}
                 case Some(value) => {
                    val area = findProperty(m,obsFrom,wf_onto_ref_area)
                    val diff = if (isHigh) value - mean else mean - value 
                    val zScore = diff / sd
                    val finalZScore : Double = 
                      if (Math.abs(zScore) < 0.001) 0.0
                      else zScore
                    newModel.add(obsTo,cex_value,literalDouble(finalZScore))
                 }
               }
               
               newModel.add(obsTo,qb_dataSet,datasetTo)
               newModel.add(obsTo,sdmxConcept_obsStatus,cex_Normalized)
               val newC = newComp(m)
               newModel.add(obsTo,cex_computation,newC)
               newModel.add(newC,rdf_type,cex_Normalize)
               newModel.add(newC,cex_observation,obsFrom)
               newModel.add(newC,cex_mean,literalDouble(mean))
               newModel.add(newC,cex_stdDesv,literalDouble(sd))
               newModel.add(newC,cex_slice,sliceFrom)
             }
           }
         }
       }
     }
   }
   newModel
 }

 def addCluster(m:Model, year:Int, includePrimaries: Boolean) : Model = {
   println("Adding cluster..." + year + ". include: " + includePrimaries)
   val newModel = ModelFactory.createDefaultModel()
   findDatasetWithComputationYear(m, cex_ClusterDataSets,year) match {
     case None => {
       logger.warn("No dataset with computation " + cex_ClusterDataSets)
     }
   
     case Some((datasetTo,computation)) => {
         for ((sliceTo,indicatorTo) <- getDatasetSlicesIndicators(m,datasetTo)) {
           // Search datasets from which to copy observations
           for (datasetFrom <- getComputationDatasets(m,computation)) {
             for ((sliceFrom,indicatorFrom) <- getDatasetSlicesIndicators(m,datasetFrom)
                 ) {
               if (indicatorTo == indicatorFrom) {
                 if (mustBeClustered(m,sliceFrom,year,includePrimaries)) {
                  for (obsFrom <- getObservationsSlice(m,sliceFrom)) {
                   val obsTo = newObs(m,year)
                   newModel.add(sliceTo,qb_observation,obsTo)
                   copyProperties(obsTo,newModel,Seq(wf_onto_ref_area, cex_value),obsFrom,m)
                   newModel.add(obsTo,wf_onto_ref_year,literalInt(year))
                  }
                 }
               }
             }
           }
         }
     }
   }
   newModel
 }


 /**
  * If we are including primaries, then we collect the primary indicators
  * with the same year and the secondary indicators of previous year
  * Otherwise, we collect the secondary indicators of previous year
  */
 def mustBeClustered(
     m: Model, 
     slice: Resource, 
     year: Int,
     includePrimaries: Boolean) : Boolean = {
   if (includePrimaries) {
    if (isPrimaryResource(m,slice)) getYear(m,slice) == year
    else getYear(m,slice) == (year - 1)
   } else {
     getYear(m,slice) == (year - 1)
   }
 }
 
 
 def groupClusters(m:Model,year:Int) : Model = {
   groupMean(m,cex_GroupClusters,wi_weightSchema_indicatorWeights,cex_Component,year,true)
 }


 def groupSubIndex(m:Model,year:Int) : Model = {
   groupMean(m,cex_GroupSubIndex,wi_weightSchema_componentWeights,cex_SubIndex,year,false)
 }

 
 def groupIndex(m:Model,year:Int) : Model = {
   groupMean(m,cex_GroupIndex,wi_weightSchema_subindexWeights,cex_Index,year,false)
 }

 def groupMean(m:Model, 
     compType: Resource, 
     weightSchema: Resource, 
     level: Resource, 
     year: Int,
     doMean: Boolean) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   findDatasetWithComputationYear(m,compType,year) match {
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
       val newTable = table.group(groupings,weights,doMean)
       saveTable(newModel,m,dataset,newTable,weightSchema)
       newModel
     }
   }
 }

 private def saveTable(newModel : Model,oldModel : Model, dataset: Resource, table: TableComputations, weights: Resource) : Unit = {
   val iterSlicesTo = oldModel.listObjectsOfProperty(dataset,qb_slice)
   while (iterSlicesTo.hasNext) {
         val sliceTo = iterSlicesTo.next.asResource
         val indicator = findProperty_asResource(oldModel,sliceTo,cex_indicator)
         val year = findProperty(oldModel,sliceTo,wf_onto_ref_year)
         for (area <- table.getAreas) {
           if (table.contains(indicator,area)) {
        	 val obs = newObs(newModel,year.asLiteral.getInt)
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

 private def collectWeights(m:Model, w: Resource) = {
   val weightsBuilder = Map.newBuilder[Resource,Double]
   val iterWeights = m.listObjectsOfProperty(w,cex_weight)
   while (iterWeights.hasNext) {
      val weightNode = iterWeights.nextNode().asResource()
      val element = findProperty_asResource(m,weightNode,cex_element)
      val weight  = getValue(m,weightNode)
      weightsBuilder += ((element,weight))
    }
    weightsBuilder.result
 }

 private def collectGroupings(m:Model, typeComponent: Resource) = {
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

 
 private def collectObservations(m: Model, dataset: Resource) : TableComputations = {
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
 

 private def addValueComputed(m: Model, 
     obs: Resource, 
     indicator: Resource, 
     area: Resource, 
     table: TableComputations, 
     weights: Resource) : Unit = {
   m.add(obs,cex_value,literalDouble(table.lookupValue(indicator,area)))
   val comp = newComp(m)
   m.add(obs,cex_computation,comp)
   m.add(obs,cex_indicator,indicator)
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

 def getCompScoreDataset(m:Model): Seq[Resource] = {
   m.listSubjectsWithProperty(rdf_type,cex_ScoreDataset).toList
 }
 
 def getCompSlices(m:Model, comp: Resource): Seq[Resource] = {
   m.listObjectsOfProperty(comp,cex_slice).map(_.asResource).toList
 } 
 
 def collectMinMax(m: Model, slice: Resource) : (Double,Double) = {
   var min : Double = Double.MaxValue
   var max : Double = Double.MinValue
   for (obs <- getObservationsSlice(m, slice)) {
     val value = getValue(m,obs)
     if (value < min) min = value
     if (value > max) max = value
   }
   (min,max)
 }
 
 def addScores(m:Model) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   for (comp <- getCompScoreDataset(m)) {
     val datasetToCopy = findSubject_asResource(m,comp,cex_computation)
     for (sliceTo <- getDatasetSlices(m,datasetToCopy)) {
       val year = getYear(m, sliceTo)
       val indicatorTo = findProperty(m,sliceTo,cex_indicator)
       for (sliceFrom <- getCompSlices(m,comp)) {
         val indicatorFrom = findProperty(m,sliceFrom,cex_indicator)
         if (indicatorTo == indicatorFrom) {
           val (min,max) = collectMinMax(m,sliceFrom)
           val range = max - min
           for (obs <- getObservationsSlice(m, sliceFrom)) {
             val obsTo = newObs(m,year)
             val value = getValue(m,obs)
             val newValue = 100 * (value - min) / range
             newModel.add(sliceTo,qb_observation,obsTo)
             newModel.add(obsTo,cex_value,literalDouble(newValue))
             copyProperties(obsTo, newModel, Seq(wf_onto_ref_area,wf_onto_ref_year,cex_indicator), obs, m)
             
             val comp = newComp(m)
             m.add(obsTo,cex_computation,comp)
             m.add(comp,rdf_type,cex_Score)
             m.add(comp,cex_observation,obs)
             m.add(comp,cex_slice,sliceFrom)
             m.add(comp,cex_rangeMin,literalDouble(0))
             m.add(comp,cex_rangeMax,literalDouble(100))
             m.add(comp,cex_valueMin,literalDouble(min))
             m.add(comp,cex_valueMax,literalDouble(max))
           }
         }
       }
     }
   }
        	 
   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }
 
 def addComputations(m: Model,primaryYear: Int) : Model = {

   // Removes the first year because we secondary indicators of one year are based
   // on year before
   val years = getYears(m).tail + primaryYear

   m.add(AddDatasets.normalizedDatasets(m))
   val normalize = addNormalize(m)
   println("Normalized: " + normalize.size)
   m.add(normalize)  

   for (year <- years) {
     println("Year: " + year)
     // Create datasets of indicators clustered by year 
     // in case it is the last year, add primary indicators also
     val includePrimaries = year == primaryYear
     m.add(AddDatasets.clusterIndicatorsDataset(m,year,includePrimaries))
	 val clustered = addCluster(m,year, includePrimaries)
     println("Clustered: " + clustered.size)
     m.add(clustered)
   
     // Create dataset of groups of indicators (components)
     m.add(AddDatasets.clustersGroupedDataset(m,year))
     val groups = groupClusters(m,year)
     println("Groups: " + groups.size)
     m.add(groups) 

     // Create dataset of subindex (subindexes)
     m.add(AddDatasets.subindexGroupedDataset(m,year))
     val subindexes = groupSubIndex(m,year)
     println("Subindexes: " + subindexes.size)
     m.add(subindexes) 

     // create dataset of index
     m.add(AddDatasets.compositeDataset(m,year))
	 val index = groupIndex(m,year)	
     println("Index: " + index.size)
     m.add(index) 

     println("Adding rankings datasets")
     m.add(AddDatasets.rankingsDataset(m,year))
     m.add(AddDatasets.scoresDataset(m, year))
   }

   val scores = addScores(m)
   println("Scores: " + scores.size)
   m.add(scores)
   println("Adding rankings")
   AddRanking.addRankings(m)
   m
 } 
 
 def addStep(step: Model => Model, m: Model,name: String): Unit = {
   val newModel = step(m)
   println(name + ": " + newModel.size)
   m.add(newModel)
 }

 override def main(args: Array[String]) {

  val logger 		= LoggerFactory.getLogger("Application")
  
  val conf 			= ConfigFactory.load()
  
  val opts 	= new AddComputationsOpts(args,onError)
  try {
   val model = ModelFactory.createDefaultModel
   val inputStream = FileManager.get.open(opts.fileName())
   model.read(inputStream,"","TURTLE")
   val newModel = addComputations(model,opts.year())
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
