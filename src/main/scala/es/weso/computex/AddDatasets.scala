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

object AddDatasets extends App {

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
 
 def normalizedDatasets(m:Model) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   for (dataset <- getDatasets(m)) {
     if (hasComputationType(m,dataset,cex_Imputed) || isPrimaryDataset(m,dataset)) {

       val newDataSet = newDataset(m)
       newModel.add(newDataSet,rdf_type,qb_DataSet)
       
       val computation = newComp(m)
       newModel.add(computation,rdf_type,cex_NormalizeDataSet)
       newModel.add(computation,cex_dataSet,dataset)
       newModel.add(newDataSet,cex_computation,computation)
       newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
       newModel.add(newDataSet,qb_structure,wf_onto_DSD)

       for (slice <- getSlices(m,dataset)) {
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


 def clusterIndicatorsDataset(m:Model, year:Int, includePrimaryIndicators: Boolean) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val newDataSet = mkClusterIndicators(m,year)

   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m,year)
   newModel.add(computation,rdf_type,cex_ClusterDataSets)
   newModel.add(newDataSet,cex_computation,computation)
   addFilterYear(newModel,computation,year)
   for (dataset <- getDatasets(m)) {
     if (hasComputationType(m,dataset,cex_NormalizeDataSet)) {
       newModel.add(computation,cex_dataSet,dataset)
     }
   }
   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)


   // Create one slice for each indicator
   val secondaryIndicators = getSecondaryIndicators(m)
   val indicators = 
     	if (includePrimaryIndicators) 
     	  secondaryIndicators ++ getPrimaryIndicators(m)
     	else 
     	  secondaryIndicators		
   
   for (indicator <- indicators) {
     val sliceIndicator = newSlice(m,year)
     newModel.add(sliceIndicator,rdf_type,qb_Slice)
     newModel.add(sliceIndicator,cex_indicator,indicator)
     newModel.add(sliceIndicator,wf_onto_ref_year,literalInt(year))
     newModel.add(newDataSet,qb_slice,sliceIndicator)
   }
   
   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }

 def addFilterYear(m:Model,comp: Resource, year:Int) : Unit = {
   m.add(comp,cex_filterDimension,wf_onto_ref_year)
   m.add(comp,cex_filterValue,literalInt(year))
 }

  def clustersGroupedDataset(m:Model,year:Int) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val newDataSet = mkClustersGrouped(m,year)
   
   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m,year)
   newModel.add(computation,rdf_type,cex_GroupClusters)
   newModel.add(computation,cex_dataSet,mkClusterIndicators(m, year))
   newModel.add(computation,cex_dimension,wf_onto_ref_area)
   addFilterYear(newModel,computation,year)

   newModel.add(newDataSet,cex_computation,computation)
   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)

   for (component <- getComponents(m)) {
     newModel.add(computation,cex_component,component)

     val slice = newSlice(m,year)
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
   val newDataSet = mkSubIndexGrouped(m,year) 
   
   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m,year)
   newModel.add(computation,rdf_type,cex_GroupSubIndex)
   newModel.add(computation,cex_dataSet,mkClustersGrouped(m,year))
   newModel.add(computation,cex_dimension,wf_onto_ref_area)
   addFilterYear(newModel,computation,year)

   newModel.add(newDataSet,cex_computation,computation)

   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)
   
   // Collect subindexes
   val iterSubindexes = m.listSubjectsWithProperty(rdf_type,cex_SubIndex)
   while (iterSubindexes.hasNext) {
     val subindex = iterSubindexes.nextResource()
     newModel.add(computation,cex_component,subindex)
     
     val slice = mkSlice(newModel,subindex,year)
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
   val newDataSet = mkComposite(m,year) 
   
   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m,year)
   newModel.add(computation,rdf_type,cex_GroupIndex)
   newModel.add(computation,cex_dataSet,mkSubIndexGrouped(m,year))
   newModel.add(computation,cex_dimension,wf_onto_ref_area)
   addFilterYear(newModel,computation,year)

   newModel.add(newDataSet,cex_computation,computation)

   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)
   
   val index = newModel.createResource(wi_index_index.getURI)
   newModel.add(computation,cex_component,index)
     
   val slice = mkSlice(m,"Composite",year)
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
   val newDataSet = mkRankings(m,year)
   
   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m,year)
   newModel.add(computation,rdf_type,cex_RankingDataset)
   newModel.add(computation,cex_dimension,wf_onto_ref_area)
   newModel.add(newDataSet,cex_computation,computation)

   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)

   val iterSubindexes = m.listSubjectsWithProperty(rdf_type,cex_SubIndex)
   while (iterSubindexes.hasNext) {
     val subindex = iterSubindexes.nextResource
     newModel.add(computation,cex_slice,mkSlice(m,subindex,year))
     val sliceToRank = mkRanking(m,subindex,year)
     newModel.add(newDataSet,qb_slice,sliceToRank)
     newModel.add(sliceToRank,rdf_type,qb_slice)
     newModel.add(sliceToRank,cex_indicator,subindex)
     newModel.add(sliceToRank,wf_onto_ref_year,literalInteger(year))
     newModel.add(sliceToRank,qb_sliceStructure,wf_onto_sliceByArea)
   }
   
   val sliceRankComposite = mkRanking(m,"Composite",year)
   newModel.add(computation,cex_slice,mkSlice(m,"Composite",year))
   newModel.add(newDataSet,qb_slice,sliceRankComposite)
   newModel.add(sliceRankComposite,rdf_type,qb_slice)
   newModel.add(sliceRankComposite,cex_indicator,wi_index_index)
   newModel.add(sliceRankComposite,wf_onto_ref_year,literalInteger(year))
   newModel.add(sliceRankComposite,qb_sliceStructure,wf_onto_sliceByArea)

   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }

 def scoresDataset(m:Model,year:Int) : Model = {
   val newModel = ModelFactory.createDefaultModel()
   val newDataSet = mkScores(m,year)
   
   newModel.add(newDataSet,rdf_type,qb_DataSet)
   
   val computation = newComp(m,year)
   newModel.add(computation,rdf_type,cex_ScoreDataset)
   newModel.add(computation,cex_dimension,wf_onto_ref_area)
   newModel.add(newDataSet,cex_computation,computation)

   newModel.add(newDataSet,sdmxAttribute_unitMeasure,dbpedia_Year)
   newModel.add(newDataSet,qb_structure,wf_onto_DSD)

   val iterSubindexes = m.listSubjectsWithProperty(rdf_type,cex_SubIndex)
   while (iterSubindexes.hasNext) {
     val subindex = iterSubindexes.nextResource
     newModel.add(computation,cex_slice,mkSlice(m,subindex,year))
     val sliceToScore = mkScore(m,subindex,year)
     newModel.add(newDataSet,qb_slice,sliceToScore)
     newModel.add(sliceToScore,rdf_type,qb_slice)
     newModel.add(sliceToScore,cex_indicator,subindex)
     newModel.add(sliceToScore,wf_onto_ref_year,literalInteger(year))
     newModel.add(sliceToScore,qb_sliceStructure,wf_onto_sliceByArea)
   }
   
   val sliceScoreComposite = mkScore(m,"Composite",year)
   newModel.add(computation,cex_slice,mkSlice(m,"Composite",year))
   newModel.add(newDataSet,qb_slice,sliceScoreComposite)
   newModel.add(sliceScoreComposite,rdf_type,qb_slice)
   newModel.add(sliceScoreComposite,cex_indicator,wi_index_index)
   newModel.add(sliceScoreComposite,wf_onto_ref_year,literalInteger(year))
   newModel.add(sliceScoreComposite,qb_sliceStructure,wf_onto_sliceByArea)

   newModel.setNsPrefixes(PREFIXES.cexMapping)
   newModel
 }
} 
