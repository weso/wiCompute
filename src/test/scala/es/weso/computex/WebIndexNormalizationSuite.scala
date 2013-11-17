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

class WebIndexNormalizationSuite extends FunSpec 
	with ShouldMatchers 
	with ComputeUtilsSuite {

  val model = JenaUtils.parseFromURI("file:examples/WebIndexDataset.ttl")
  val expanded = AddComputations.addComputations(model,2013)


  def ValidateNormalizedValues(country: String) : Unit = {

    describe("Validate all normalized values for " + country) {
      info("Validating " + country)
      val map = getTableValues("normalizedValues"+country)
      map.keys.foreach {
        k => { 
          it("Should validate " + k + " for country " + country) {
            val year = if (isPrimary(expanded,k)) 2013 
            		   else 2012
            map(k) match {
              case None => findValueCompType(expanded,k,year,country,cex_Normalize) should be(None)
              case Some(d) => matchDoublesCompType(expanded,k,year,country,cex_Normalize,d)
            }
          }
        }
     }
    }
  
  }
  
  ValidateNormalizedValues("ARG")
  ValidateNormalizedValues("ESP")
}