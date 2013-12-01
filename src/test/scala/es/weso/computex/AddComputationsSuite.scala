package es.weso.computex

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import es.weso.utils.JenaUtils._
import es.weso.utils.JenaUtils
import CexUtils._
import PREFIXES._

class AddComputationsSuite extends FunSpec 
	with ShouldMatchers {

  val epsilon: Float = (0.001).toFloat
  
  describe("Add computations") {
    val model = JenaUtils.parseFromURI("file:examples/example-imputed.ttl")
    val expanded = AddComputations.addComputations(model,2012)

    it("Should maintain imputed values") {
      findValueInDataset(expanded,"A",2009,"ESP","A-Imputed") should be (Some(2.0))
    }

    it("Should normalize observations - A 09 ESP") {
      findValueCompType(expanded,"A",2009,"ESP",cex_Normalize) should be (Some(0.0))
    }

    it("Should normalize observations - Q1 2011 ESP") {
      findValueCompType(expanded,"Q1",2011,"ESP",cex_Normalize) should be (Some(-1.0))
    }

    it("Should normalize observations - Q2 2011 FIN") {
      matchDoubles(findValueCompType(expanded,"Q2",2011,"FIN",cex_Normalize).get,-0.800641)
    }
    
    def matchDoubles(d1: Double, d2: Double) = {
      d1.toFloat should be(d2.toFloat plusOrMinus epsilon)
    }
}
  
  
}