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

class WebIndexComponentsSuite extends FunSpec 
	with ShouldMatchers 
	with ComputeUtilsSuite {

  val model = JenaUtils.parseFromURI("file:examples/WebIndexDataset.ttl")
  val expanded = AddComputations.addComputations(model,2013)


  def ValidateComponentValues(country: String,year:Int) : Unit = {

    describe("Validate all component values for " + country + " and year " + year) {
      info("Validating " + country)
      val map = getTableValues("componentValues" + country+"_"+year)
      map.keys.foreach {
        k => { 
          it("Should validate " + k + " for country " + country) {
            val i = findComponent(expanded,k).get
            val c = findCountry(expanded,country).get
            map(k) match {
              case None => findValueCompType(expanded,i,year,c,cex_GroupMean) should be(None)
              case Some(d) => matchDoublesCompType(expanded,i,year,c,cex_GroupMean,d)
            }
          }
       }
      }
    }
      
    describe("Validate subindex values for " + country + " and year " + year) {
      val map = getTableValues("subindexValues" + country+"_"+year)
      map.keys.foreach {
        k => { 
          it("Should validate " + k + " for country " + country) {
            val i = if (k=="composite") wi_index_index
                    else findSubIndex(expanded,k).get
            val c = findCountry(expanded,country).get
            map(k) match {
              case None => findValueCompType(expanded,i,year,c,cex_GroupMean) should be(None)
              case Some(d) => matchDoublesCompType(expanded,i,year,c,cex_GroupMean,d)
            }
          }
          
        }
     }
      
    
    }
    
  }
  
  ValidateComponentValues("ARG",2008)
  ValidateComponentValues("ARG",2013)
  ValidateComponentValues("ARG",2012)
  ValidateComponentValues("ESP",2013)
}