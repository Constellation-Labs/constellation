package org.constellation.infrastructure.endpoints

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import monocle.macros.GenLens
import org.constellation.domain.redownload.RedownloadService
import org.constellation.gossip.sampling.GossipPath
import org.constellation.gossip.snapshot.SnapshotProposalGossipService
import org.constellation.gossip.state.GossipMessage
import org.constellation.gossip.validation.{MessageValidationError, MessageValidator}
import org.constellation.keytool.KeyUtils
import org.constellation.schema.Id
import org.constellation.schema.signature.Signed.signed
import org.constellation.schema.snapshot.{HeightRange, MajorityInfo, SnapshotProposal, SnapshotProposalPayload}
import org.constellation.serialization.KryoSerializer
import org.constellation.session.Registration.`X-Id`
import org.http4s.Status.{BadRequest, Ok}
import org.http4s.implicits._
import org.http4s.{Header, HttpRoutes, Method, Request}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.SortedMap
import scala.concurrent.ExecutionContext

class SnapshotEndpointsTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with BeforeAndAfterAll {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val snapshotProposalGossipService = mock[SnapshotProposalGossipService[IO]]
  private val redownloadService = mock[RedownloadService[IO]]
  private val messageValidator = mock[MessageValidator]

  private val originKeyPair = KeyUtils.makeKeyPair()
  private val originId = Id.fromPublicKey(originKeyPair.getPublic)
  private val selfId = Id("self")

  override def beforeAll: Unit = KryoSerializer.init[IO].handleError(_ => Unit).unsafeRunSync()

  before {
    redownloadService.addPeerProposal(*, *) shouldReturnF Unit
    redownloadService.updatePeerMajorityInfo(*, *) shouldReturnF Unit
    snapshotProposalGossipService.spread(*[GossipMessage[SnapshotProposalPayload]]) shouldReturnF Unit
  }

  "postSnapshotProposal" - {

    val postSnapshotProposal: HttpRoutes[IO] = new SnapshotEndpoints[IO]().postSnapshotProposal(
      snapshotProposalGossipService,
      redownloadService,
      messageValidator
    )

    val validMessage = GossipMessage(
      payload = SnapshotProposalPayload(
        signed(
          SnapshotProposal("proposal-hash", 50L, SortedMap.empty[Id, Double]),
          originKeyPair
        ),
        MajorityInfo(HeightRange(10L, 50L), List.empty)
      ),
      path = GossipPath(IndexedSeq(originId, selfId, originId), "path-id", 1),
      originId
    )

    messageValidator.validateForForward[SnapshotProposalPayload](*, *).shouldAnswer {
      (message: GossipMessage[SnapshotProposalPayload], _: Id) =>
        message.valid[MessageValidationError]
    }

    "should respond with Ok when proposal passes validation" in {
      val request = Request[IO]()
        .withMethod(Method.POST)
        .withUri(uri"/peer/snapshot/created")
        .withEntity(validMessage)
        .putHeaders(Header(`X-Id`.value, originId.hex))

      val response = postSnapshotProposal.orNotFound.run(request).unsafeRunSync()

      response.status shouldBe Ok
    }

    "should respond with BadRequest when proposal fails validation" in {
      val invalidProposalSignatureMessage =
        GenLens[GossipMessage[SnapshotProposalPayload]](_.payload.proposal.signature.signature)
          .set("invalid-signature")(validMessage)

      val request = Request[IO]()
        .withMethod(Method.POST)
        .withUri(uri"/peer/snapshot/created")
        .withEntity(invalidProposalSignatureMessage)
        .putHeaders(Header(`X-Id`.value, originId.hex))

      val response = postSnapshotProposal.orNotFound.run(request).unsafeRunSync()

      response.status shouldBe BadRequest
    }
  }

}
