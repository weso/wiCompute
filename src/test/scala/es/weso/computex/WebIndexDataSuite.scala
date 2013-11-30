package es.weso.computex

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import es.weso.utils.JenaUtils._
import es.weso.utils.JenaUtils
import CexUtils._
import PREFIXES._
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.Resource

class WebIndexDataSuite extends FunSpec 
	with ShouldMatchers with ComputeUtilsSuite {

  
  describe("Add computations") {
    val model = JenaUtils.parseFromURI("file:examples/WebIndexDataset.ttl")
    val expanded = AddComputations.addComputations(model,2013)

    it("Should maintain imputed values") {
      matchDoublesDataset(expanded,"ITU_R",2012,"ARG","ITU_R-Imputed",0.0847)
    }

    it("Check Normalized ARG Q11 ") {
      matchDoublesCompType(expanded,"Q11",2013,"ARG",cex_Normalize,0.778)
    }

    it("Check Normalized ARG Q12 ") {
      matchDoublesCompType(expanded,"Q12",2013,"ARG",cex_Normalize,1.497)
    }

    it("Check Imputed ESP WB-H 2012") {
      matchDoublesDataset(expanded,"WB_H",2012,"ESP","WB_H-Imputed",1.046)
    }

    it("Check Normalized ESP WB-H 2012") {
      matchDoublesCompType(expanded,"WB_H",2012,"ESP",cex_Normalize,0.329)
    }

    it("Check Imputed AUS ITU_G 2012") {
      matchDoublesDataset(expanded,"ITU_G",2012,"AUS","ITU_G-Imputed",99)
    }

    it("Check Normalized AUS ITU_G 2012") {
      matchDoublesCompType(expanded,"ITU_G",2012,"AUS",cex_Normalize,0.385)
    }

    it("Check Imputed AUS WI_D 2012") {
      findValueInDataset(expanded,"WI_D",2012,"AUS","WI_D-Imputed") should be(None)
    }

    it("Check Normalize AUS WI_D 2012") {
      findValueCompType(expanded,"WI_D",2012,"AUS",cex_Normalize) should be(None)
    }

    it("Check Normalize CHL WI_D 2012") {
      matchDoublesCompType(expanded,"WI_D",2012,"CHL",cex_Normalize,0.379)
    }

/*    it("Check Group CHL Affordability 2012") {
      matchDoublesCompType(expanded,wi_component + "affordability",2013,"CHL",cex_Normalize,0.379)
    } */

}
  
  
}