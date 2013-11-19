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
import CexUtils._

class AddDatasetsOpts(arguments: Array[String],
    onError: (Throwable, Scallop) => Nothing
    ) extends ScallopConf(arguments) {

    banner("""| Generate Computation Datasets
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
    				required = true, 
    				descr = "Year of index")
    val version = opt[Boolean]("version", 
    				noshort = true, 
    				descr = "Print version")
    val help 	= opt[Boolean]("help", 
    				noshort = true, 
    				descr = "Show this message")
  
  override protected def onError(e: Throwable) = onError(e, builder)
}

object AddDatasets extends App {

 lazy val patternPrimaryDatasets = """Q\d{1,3}-Ordered""".r
 
  
 def imputedDatasets(m:Model) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val iter = m.listSubjectsWithProperty(rdf_type,qb_DataSet)
   
   while (iter.hasNext) {
     val dataset = iter.nextResource()
     val newDataSet = newDataset(m)
     newModel.add(newDataSet,rdf_type,qb_DataSet)
     
     val computation = newComp(m)
     newModel.add(computation,rdf_type,cex_ImputeDataSet)
     newModel.add(computation,cex_method,cex_AvgGrowth2Missing)
     newModel.add(computation,cex_method,cex_MeanBetweenMissing)
     newModel.add(computation,cex_method,cex_CopyRaw)
     newModel.add(computation,cex_dataSet,dataset)
     
     newModel.add(newDataSet,cex_computation,computation)
     newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
     newModel.add(newDataSet,qb_structure,wf_onto_DSD)

     val iterSlices = m.listStatements(dataset,qb_slice,null : RDFNode)
     while (iterSlices.hasNext) {
       val slice = iterSlices.next.getObject().asResource()
       val newS = newSlice(m)
       newModel.add(newS,rdf_type,qb_Slice)
       newModel.add(newS,cex_indicator,findProperty_asResource(m,slice,cex_indicator))
       newModel.add(newS,wf_onto_ref_year,findProperty_asLiteral(m,slice,wf_onto_ref_year))
       newModel.add(newS,qb_sliceStructure,wf_onto_sliceByArea)
       newModel.add(newDataSet,qb_slice,newS)
     }
   }
   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }
 
 def isPrimaryIndicator(m: Model, indicator : Resource) = {
   m.contains(indicator,rdf_type,wf_onto_PrimaryIndicator)
 }
 
 def isPrimaryDataset(m : Model, d: Resource) : Boolean = {
   var isPrimary = false
   if (hasProperty(m,d,qb_slice)) {
     val iterSlices = m.listObjectsOfProperty(d,qb_slice)
     while (iterSlices.hasNext) {
       val slice = iterSlices.next.asResource
       val indicator = findProperty_asResource(m,slice,cex_indicator)
       if (isPrimaryIndicator(m, indicator)) isPrimary = true
     }
   } 
   isPrimary
 }
 
 def normalizedDatasets(m:Model) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val datasetsIter = m.listSubjectsWithProperty(rdf_type,qb_DataSet)
   
   while (datasetsIter.hasNext) {
     val dataset = datasetsIter.nextResource()
     if (hasComputationType(m,dataset,cex_Imputed) || isPrimaryDataset(m,dataset)) {

       val newDataSet = newDataset(m)
       newModel.add(newDataSet,rdf_type,qb_DataSet)
       
       val computation = newComp(m)
       newModel.add(computation,rdf_type,cex_NormalizeDataSet)
       newModel.add(computation,cex_dataSet,dataset)
       newModel.add(newDataSet,cex_computation,computation)
       newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
       newModel.add(newDataSet,qb_structure,wf_onto_DSD)

       val iterSlices = m.listStatements(dataset,qb_slice,null : RDFNode)
       while (iterSlices.hasNext) {
         val slice = iterSlices.next.getObject().asResource()
         val newS = newSlice(m)
         newModel.add(newS,rdf_type,qb_Slice)
         newModel.add(newS,cex_indicator,findProperty_asResource(m,slice,cex_indicator))
         newModel.add(newS,wf_onto_ref_year,findProperty_asLiteral(m,slice,wf_onto_ref_year))
         newModel.add(newS,qb_sliceStructure,wf_onto_sliceByArea)
         newModel.add(newDataSet,qb_slice,newS)
       }
     }
   }
   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }

 def adjustedDatasets(m:Model) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val datasetsIter = m.listSubjectsWithProperty(rdf_type,qb_DataSet)
   
   while (datasetsIter.hasNext) {
     val dataset = datasetsIter.nextResource()
     val computation = findProperty_asResource(m,dataset,cex_computation)
     val typeComputation = findProperty(m,computation,rdf_type)
     if (typeComputation == cex_NormalizeDataSet) {
       val newDataSet = newDataset(m)
       newModel.add(newDataSet,rdf_type,qb_DataSet)
       
       val computation = newComp(m)
       newModel.add(computation,rdf_type,cex_AdjustDataSet)
       newModel.add(computation,cex_dataSet,dataset)
       newModel.add(newDataSet,cex_computation,computation)
       newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
       newModel.add(newDataSet,qb_structure,wf_onto_DSD)

       val iterSlices = m.listStatements(dataset,qb_slice,null : RDFNode)
       while (iterSlices.hasNext) {
         val slice = iterSlices.next.getObject().asResource()
         val newS = newSlice(m)
         newModel.add(newS,rdf_type,qb_Slice)
         newModel.add(newS,cex_indicator,findProperty_asResource(m,slice,cex_indicator))
         newModel.add(newS,wf_onto_ref_year,findProperty_asLiteral(m,slice,wf_onto_ref_year))
         newModel.add(newS,qb_sliceStructure,wf_onto_sliceByArea)
         newModel.add(newDataSet,qb_slice,newS)
       }
     }
   }
   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }

 def clusterIndicatorsDataset(m:Model, year:Int) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val newDataSet = wi_dataset_ClusterIndicators

   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m)
   newModel.add(computation,rdf_type,cex_ClusterDataSets)
   newModel.add(newDataSet,cex_computation,computation)

   // Collect normalized datasets
   val iterDatasets = m.listSubjectsWithProperty(rdf_type,qb_DataSet)
   while (iterDatasets.hasNext) {
     val dataset = iterDatasets.nextResource
     if (hasComputationType(m,dataset,cex_NormalizeDataSet)) {
       newModel.add(computation,cex_dataSet,dataset)
     }
   }

   
   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)

   val dim = wf_onto_ref_year
   val valueDim = literalInteger(year)
   newModel.add(computation,cex_dimension,dim)
   newModel.add(computation,cex_value,valueDim)

   // Create one slice for each indicator
   val iterIndicators = m.listSubjectsWithProperty(rdf_type,cex_Indicator)
   while (iterIndicators.hasNext) {
     val indicator = iterIndicators.nextResource
     val sliceIndicator = newSlice(m)
     newModel.add(sliceIndicator,rdf_type,qb_Slice)
     newModel.add(sliceIndicator,cex_indicator,indicator)
     newModel.add(sliceIndicator,dim,valueDim)
     newModel.add(newDataSet,qb_slice,sliceIndicator)
   }
   
   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }

  def clustersGroupedDataset(m:Model,year:Int) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val newDataSet = wi_dataset_ClustersGrouped 
   
   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m)
   newModel.add(computation,rdf_type,cex_GroupClusters)
   newModel.add(computation,cex_dataSet,wi_dataset_ClusterIndicators)
   newModel.add(computation,cex_dimension,wf_onto_ref_area)

   
   newModel.add(newDataSet,cex_computation,computation)

   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)
   
   // Collect components
   val iterComponents = m.listSubjectsWithProperty(rdf_type,cex_Component)
   while (iterComponents.hasNext) {
     val component = iterComponents.nextResource()
     newModel.add(computation,cex_component,component)
     
     val slice = newSlice(m)
     newModel.add(slice,rdf_type,qb_Slice)
     newModel.add(slice,cex_indicator,component)
     newModel.add(slice,wf_onto_ref_year,literalInteger(year))
     newModel.add(slice,qb_sliceStructure,wf_onto_sliceByArea)
     newModel.add(newDataSet,qb_slice,slice)
   }

   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }


  def subindexGroupedDataset(m:Model,year:Int) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val newDataSet = wi_dataset_SubIndexGrouped 
   
   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m)
   newModel.add(computation,rdf_type,cex_GroupSubIndex)
   newModel.add(computation,cex_dataSet,wi_dataset_ClustersGrouped)
   newModel.add(computation,cex_dimension,wf_onto_ref_area)

   newModel.add(newDataSet,cex_computation,computation)

   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)
   
   // Collect subindexes
   val iterSubindexes = m.listSubjectsWithProperty(rdf_type,cex_SubIndex)
   while (iterSubindexes.hasNext) {
     val subindex = iterSubindexes.nextResource()
     newModel.add(computation,cex_component,subindex)
     
     val slice = mkSlice(newModel,subindex)
     newModel.add(slice,rdf_type,qb_Slice)
     newModel.add(slice,cex_indicator,subindex)
     newModel.add(slice,wf_onto_ref_year,literalInteger(year))
     newModel.add(slice,qb_sliceStructure,wf_onto_sliceByArea)
     newModel.add(newDataSet,qb_slice,slice)
   }

   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }

 def compositeDataset(m:Model,year:Int) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val newDataSet = wi_dataset_Composite 
   
   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m)
   newModel.add(computation,rdf_type,cex_GroupIndex)
   newModel.add(computation,cex_dataSet,wi_dataset_SubIndexGrouped)
   newModel.add(computation,cex_dimension,wf_onto_ref_area)

   newModel.add(newDataSet,cex_computation,computation)

   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)
   
   val index = newModel.createResource(wi_index_index.getURI)
   newModel.add(computation,cex_component,index)
     
   val slice = mkSlice(m,"Composite")
   newModel.add(slice,rdf_type,qb_Slice)
   newModel.add(slice,cex_indicator,index)
   newModel.add(slice,wf_onto_ref_year,literalInteger(year))
   newModel.add(slice,qb_sliceStructure,wf_onto_sliceByArea)
   newModel.add(newDataSet,qb_slice,slice)

   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }

 def rankingsDataset(m:Model,year:Int) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val newDataSet = wi_dataset_Rankings 
   
   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m)
   newModel.add(computation,rdf_type,cex_RankingDataset)
   newModel.add(computation,cex_dimension,wf_onto_ref_area)
   newModel.add(newDataSet,cex_computation,computation)

   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)

   val iterSubindexes = m.listSubjectsWithProperty(rdf_type,cex_SubIndex)
   while (iterSubindexes.hasNext) {
     val subindex = iterSubindexes.nextResource
     newModel.add(computation,cex_slice,mkSlice(m,subindex))
     val sliceToRank = mkRanking(m,subindex)
     newModel.add(newDataSet,qb_slice,sliceToRank)
     newModel.add(sliceToRank,rdf_type,qb_slice)
     newModel.add(sliceToRank,cex_indicator,subindex)
     newModel.add(sliceToRank,wf_onto_ref_year,literalInteger(year))
     newModel.add(sliceToRank,qb_sliceStructure,wf_onto_sliceByArea)
   }
   
   val sliceRankComposite = mkRanking(m,"Composite")
   newModel.add(computation,cex_slice,mkSlice(m,"Composite"))
   newModel.add(newDataSet,qb_slice,sliceRankComposite)
   newModel.add(sliceRankComposite,rdf_type,qb_slice)
   newModel.add(sliceRankComposite,cex_indicator,wi_index_index)
   newModel.add(sliceRankComposite,wf_onto_ref_year,literalInteger(year))
   newModel.add(sliceRankComposite,qb_sliceStructure,wf_onto_sliceByArea)

   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }

 def addDatasets(m: Model, year: Int) : Model = {
   m.add(normalizedDatasets(m))
   m.add(clusterIndicatorsDataset(m,year))
   m.add(clustersGroupedDataset(m,year))
   m.add(subindexGroupedDataset(m,year))
   m.add(compositeDataset(m,year))
   m.add(rankingsDataset(m,year))
 } 

 override def main(args: Array[String]) {

  val logger 		= LoggerFactory.getLogger("Application")
  val conf 			= ConfigFactory.load()
  
  val opts 	= new AddDatasetsOpts(args,onError)
  try {
   val model = ModelFactory.createDefaultModel
   val inputStream = FileManager.get.open(opts.fileName())
   model.read(inputStream,"","TURTLE")
   val newModel = addDatasets(model,opts.year())
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
