package org.constellation

import akka.actor.ActorSystem
import akka.testkit.TestKit
import constellation.{pubKeyToAddress, pubKeysToAddress}
import org.constellation.Fixtures.{tempKey, tempKey1, tempKey2, tempKey3}
import org.constellation.primitives.Schema.{Bundle, BundleData, TX, TXData}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import constellation._

/**
  * Created by Wyatt on 5/10/18.
  */

class CellTest extends TestKit(ActorSystem("ConsensusTest")) with FlatSpecLike with BeforeAndAfterAll {
  val address1 = pubKeysToAddress(Seq(tempKey.getPublic, tempKey1.getPublic))
  val address2 = pubKeyToAddress(tempKey2.getPublic)
  val dst = pubKeyToAddress(tempKey3.getPublic)

  val tx = TX(
    TXData(
      Seq(address1, address2),
      dst,
      1L,
      keyMap = Seq(0, 0, 1)
    ).multiSigned()(Seq(tempKey, tempKey1, tempKey2))
  )


  val b = Bundle(BundleData(Seq(tx)).signed()(tempKey))

  val bb = Bundle(BundleData(Seq(b, tx)).signed()(tempKey1))

  val bbb = Bundle(BundleData(Seq(b, bb, tx)).signed()(tempKey2))

  "Cell hylomorphism" should "not recurse" in {
    val test = Sheaf(Some(bbb))
    val res = Cell.ioF(test)
    assert(res === Sheaf(None))
  }

  "Cell hylomorphism" should "recurse once" in {
    val test = Sheaf(Some(bbb))
    val res = Cell.ioF(test)
    assert(res === Sheaf(None))
  }

  "Cell hylomorphism" should "merge bundles" in {
    val test = Sheaf(Some(bbb))
    val res = Cell.ioF(test)
    assert(res === Sheaf(None))
  }

  "Cell metamorphism" should "not recurse" in {
    val test = Sheaf(Some(b))
    val res = Cell.liftF(SingularHomology(test))
    assert(res === SingularHomology(test))
  }

  "Cell metamorphism" should "merge bundles" in {
    val test = Sheaf(Some(b))
    val homotopy = Homology(Sheaf(None), test)
    val res = Cell.liftF(homotopy)
    assert(res === Homology(Sheaf(None), Sheaf(None)))
  }
}
