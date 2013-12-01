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

class SimpleComponentsSuite extends FunSpec 
	with ShouldMatchers 
	with ComputeUtilsSuite {

  val year = 2012
  val model = JenaUtils.parseFromURI("file:examples/example-imputed.ttl")
  val expanded = AddComputations.addComputations(model,year)
  JenaUtils.model2File(expanded, "examples/simple-generated.ttl", "TURTLE")
  
  def ValidateComponentValues(country: String) : Unit = {

    describe("Validate all component values for " + country) {
      info("Validating " + country)
      val map = getTableValues("simple_componentValues" + country)
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
      
    describe("Validate subindex values for " + country) {
      val map = getTableValues("simple_subindexValues" + country)
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
  
  ValidateComponentValues("ESP")
//  ValidateComponentValues("FIN")
//  ValidateComponentValues("CHL")
}