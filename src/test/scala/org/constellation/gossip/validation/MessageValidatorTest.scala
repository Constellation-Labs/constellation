package org.constellation.gossip.validation

import cats.effect.{ContextShift, IO}
import constellation.PublicKeyExt
import org.constellation.gossip.sampling.GossipPath
import org.constellation.gossip.state.GossipMessage
import org.constellation.keytool.KeyUtils
import org.constellation.schema.signature.HashSignature
import org.constellation.serializer.KryoSerializer
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class MessageValidatorTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  var validator: MessageValidator = _

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val aKp = KeyUtils.makeKeyPair()
  val bKp = KeyUtils.makeKeyPair()
  val cKp = KeyUtils.makeKeyPair()
  val dKp = KeyUtils.makeKeyPair()
  val eKp = KeyUtils.makeKeyPair()

  val aId = aKp.getPublic.toId
  val bId = bKp.getPublic.toId
  val cId = cKp.getPublic.toId
  val dId = dKp.getPublic.toId
  val eId = eKp.getPublic.toId

  KryoSerializer.init[IO].unsafeRunSync()

  before {
//    KryoSerializer.init[IO].unsafeRunSync()
    //    cluster.getPeerInfo shouldReturn peerInfo.pure[IO]
    //    peerSampling = new RandomPeerSampling(selfId, cluster)
  }

  "validates" - {
    "when signature is correct" in {
      val eValiator = new MessageValidator(eId)
      val path = GossipPath(IndexedSeq(aId, bId, cId, dId, eId, aId), "")
      val msg: GossipMessage[String] = GossipMessage("aProposal", path, aId)
        .sign(aKp)
        .sign(bKp)
        .sign(cKp)
        .sign(dKp)

      val result = eValiator.validateForForward(msg, dId)
      result.isValid shouldBe true
    }
  }

  "invalidates" - {
    "when sender incorrect" in {
      val eValiator = new MessageValidator(eId)
      val path = GossipPath(IndexedSeq(aId, bId, cId, dId, eId, aId), "")
      val msg: GossipMessage[String] = GossipMessage("aProposal", path, aId)
        .sign(aKp)
        .sign(bKp)
        .sign(cKp)
        .sign(dKp)

      val result = eValiator.validateForForward(msg, bId)
      result.isValid shouldBe false
    }

    "when receiver incorrect" in {
      val eValiator = new MessageValidator(aId)
      val path = GossipPath(IndexedSeq(aId, bId, cId, dId, eId, aId), "")
      val msg: GossipMessage[String] = GossipMessage("aProposal", path, aId)
        .sign(aKp)
        .sign(bKp)
        .sign(cKp)
        .sign(dKp)

      val result = eValiator.validateForForward(msg, dId)
      result.isValid shouldBe false
    }

    "when signature chain incorrect" - {
      "because peer signed twice" in {
        val eValiator = new MessageValidator(eId)
        val path = GossipPath(IndexedSeq(aId, bId, cId, dId, eId, aId), "")
        val msg: GossipMessage[String] = GossipMessage("aProposal", path, aId)
          .sign(aKp)
          .sign(bKp)
          .sign(dKp)
          .sign(dKp)

        val result = eValiator.validateForForward(msg, dId)
        result.isValid shouldBe false
      }

      "because signature is missing" in {
        val eValiator = new MessageValidator(eId)
        val path = GossipPath(IndexedSeq(aId, bId, cId, dId, eId, aId), "")
        val msg: GossipMessage[String] = GossipMessage("aProposal", path, aId)

        val result = eValiator.validateForForward(msg, dId)
        result.isValid shouldBe false
      }

      "because peer malformed signature" in {
        val eValiator = new MessageValidator(eId)
        val path = GossipPath(IndexedSeq(aId, bId, cId, dId, eId, aId), "")
        val msg: GossipMessage[String] = GossipMessage("aProposal", path, aId)
          .sign(aKp)
          .sign(bKp)
          .sign(cKp)
          .sign(dKp)

        val malformed = msg.copy(
          signatures = msg.signatures.init ++ IndexedSeq(HashSignature("123", dId))
        )

        val result = eValiator.validateForForward(malformed, dId)
        result.isValid shouldBe false
      }
    }
  }
}
