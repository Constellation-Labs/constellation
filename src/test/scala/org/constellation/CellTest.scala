package org.constellation

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
/**
  * Created by Wyatt on 5/10/18.
  */
class CellTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike with BeforeAndAfterAll {

  "Cell hylomorphism" should "not recurse" in {
    val test = Sheaf(None, 0)
    val res = Cell.ioF(test)
    assert(res === Sheaf(None, -11))
  }

  "Cell hylomorphism" should "recurse once" in {
    val test = Sheaf(None, 1)
    val res = Cell.ioF(test)
    assert(res === Sheaf(None, 1))
  }

  "Cell metamorphism" should "not recurse" in {
    val test = Sheaf(None, 0)
    val res = Cell.lift(Bundle(test))
    assert(res === Bundle(Sheaf(None, 0)))
  }

  "Cell metamorphism" should "recurse once" in {
    val test = Sheaf(None, 1)
    val res = Cell.lift(Bundle(test))
    assert(res === Bundle(Sheaf(None, 1)))
  }
}
