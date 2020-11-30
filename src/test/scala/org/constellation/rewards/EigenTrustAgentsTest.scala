package org.constellation.rewards

import cats.effect.{ContextShift, IO}
import org.constellation.serializer.KryoSerializer
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class EigenTrustAgentsTest
    extends AnyFreeSpec
    with BeforeAndAfter
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  before {
    KryoSerializer.init[IO].unsafeRunSync()
  }

  "should register agent" in {
    val agents = EigenTrustAgents.empty()
    val addr = "DAGfoo"
    val updated = agents.registerAgent(addr)
    updated.contains(addr) shouldBe true
  }

  "should unregister agent" in {
    val agents = EigenTrustAgents.empty()
    val addr = "DAGfoo"
    val updated = agents.registerAgent(addr)
    updated.contains(addr) shouldBe true
    val cleared = updated.unregisterAgent(addr)
    cleared.contains(addr) shouldBe false
  }

  "Int to address should be synchronized with address to Int" in {
    val agents = EigenTrustAgents.empty()
    val addr = "DAGfoo"
    val updated = agents.registerAgent(addr)
    val int = updated.get(addr).get

    updated.get(addr).get shouldBe int
    updated.get(int).get shouldBe addr
  }

  "Serialization/Deserialization" - {
    "should keep iterator's state after deserialization" in {
      val agents = EigenTrustAgents.empty()
      val addr1 = "DAGfoo1"
      val addr2 = "DAGfoo2"
      val addr3 = "DAGbar"

      val updated = agents.registerAgent(addr1).registerAgent(addr2)

      val serialized = KryoSerializer.serializeAnyRef(updated)
      val deserialized = KryoSerializer.deserializeCast[EigenTrustAgents](serialized)

      deserialized.getAllAsAddresses() shouldEqual Map(
        addr1 -> 1,
        addr2 -> 2
      )

      val updatedDeserialized = deserialized.registerAgent(addr3)

      updatedDeserialized.getAllAsAddresses() shouldEqual Map(
        addr1 -> 1,
        addr2 -> 2,
        addr3 -> 3
      )
    }
  }
}
