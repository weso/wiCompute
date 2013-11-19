package es.weso.computex

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import es.weso.utils.JenaUtils._
import com.hp.hpl.jena.rdf.model.ResourceFactory
import es.weso.computex.TableComputations._
import com.hp.hpl.jena.rdf.model.Resource

class TableComputationsSuite extends FunSpec 
	with ShouldMatchers {

  describe("Table of computations") {
   it("Compute simple table") {
      val A = ResourceFactory.createResource("A")
      val B = ResourceFactory.createResource("B")
      val C = ResourceFactory.createResource("C")
      val D = ResourceFactory.createResource("D")
      val E = ResourceFactory.createResource("E")

      val ESP = ResourceFactory.createResource("ESP")
      val FRA = ResourceFactory.createResource("FRA")
      val USA = ResourceFactory.createResource("USA")
      val SEA = ResourceFactory.createResource("SEA")

      val weights : Map[Resource,Double] = Map(A -> 1, B -> 0.5, C -> 0.6, D -> 0.8, E -> 1)

      val table = TableComputations.newTable
      def value(x : Double) : Option[(Double,Seq[(Resource,Double,Double)])] = Some(x,Seq())
      table.insert(A,ESP,value(2))
      table.insert(A,USA,value(4))
      
      table.insert(B,ESP,value(3))
      table.insert(B,FRA,value(4))
      table.insert(B,USA,value(5))

      table.insert(C,ESP,value(4))
      table.insert(C,FRA,value(1))
      table.insert(C,USA,value(2))

      table.insert(D,ESP,value(5))
      table.insert(D,FRA,value(2))
      table.insert(D,USA,value(7))

      table.insert(E,ESP,value(6))
      table.insert(E,FRA,value(3))
      table.insert(E,USA,value(8))

      val C1 = ResourceFactory.createResource("C1")
 	  val C2 = ResourceFactory.createResource("C2")
 	  val C3 = ResourceFactory.createResource("C3")
 	  val C4 = ResourceFactory.createResource("C4")

 	  val groupings = Map(C1 -> Set(A,B), C2 -> Set(C,D), C3 -> Set(E))
 	  val newTable = table.group(groupings,weights)
 	  
 	  newTable.lookupValue(C1,USA) should be(3.25)
 	  newTable.lookupList(C1,USA).toList should be (List((A,4,1),(B,5,0.5)))
 	  newTable.lookupValue(C3,FRA) should be(3)
 	  newTable.lookupValue(C1,FRA) should be(2)
 	  newTable.lookup(C4,FRA) should be(None)
 	  newTable.lookup(C1,SEA) should be(None)

   }
  }
}