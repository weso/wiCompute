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

class SimpleNormalizationSuite extends FunSpec 
	with ShouldMatchers 
	with ComputeUtilsSuite {

  val year = 2011
  val model = JenaUtils.parseFromURI("file:examples/example-imputed.ttl")
  val expanded = AddComputations.addComputations(model,year)


  def ValidateNormalizedValues(country: String) : Unit = {

    describe("Validate all normalized values for " + country) {
      info("Validating " + country)
      val map = getTableValues("simple_NormalizedValues"+country)
      map.keys.foreach {
        k => { 
          it("Should validate " + k + " for country " + country) {
            val yearToCheck = if (isPrimary(expanded,k)) year
            		   else year - 1
            map(k) match {
              case None => findValueCompType(expanded,k,yearToCheck,country,cex_Normalize) should be(None)
              case Some(d) => matchDoublesCompType(expanded,k,yearToCheck,country,cex_Normalize,d)
            }
          }
        }
     }
    }
  
  }
  
  ValidateNormalizedValues("ESP")
}