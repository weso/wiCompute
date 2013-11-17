package es.weso.computex

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import es.weso.utils.JenaUtils._
import es.weso.utils.JenaUtils
import CexUtils._
import PREFIXES._

class CexUtilsSuite extends FunSpec 
	with ShouldMatchers {

  describe("Find values") {
    val model = JenaUtils.parseFromURI("file:examples/example-imputed.ttl")

    it("Should get country") {
      findCountry(model,"ESP") should be ('defined)
      findCountry(model,"ESP").get.getURI should be (wi_country + "ESP")
    }

    it("Should get indicator") {
      findIndicator(model,"A") should be ('defined)
      findIndicator(model,"A").get.getURI should be (wi_indicator + "A")
    }

    it("Should get dataset") {
      findDataset(model,"A-Imputed") should be ('defined)
      findDataset(model,"A-Imputed").get.getURI should be (wi_dataset + "A-Imputed")
    }

    it("Should get observation") {
      findValueInDataset(model,"A",2009,"ESP","A-Imputed") should be ('defined)
      findValueInDataset(model,"A",2009,"ESP","A-Imputed").get should be (2.0)
    }

  }
  
  
}