package org.constellation.gossip.sampling

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.p2p.{Cluster, MajorityHeight, PeerData}
import org.constellation.schema.Id
import org.constellation.{PeerMetadata, ResourceInfo}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class RandomPeerSamplingTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  val selfId = Id("a")
  var peerSampling: PeerSampling[IO] = _
  var cluster: Cluster[IO] = mock[Cluster[IO]]

  val ids = Set(Id("b"), Id("c"), Id("d"), Id("e"), Id("f"), Id("g"), Id("h"))

  val peerInfo: Map[Id, PeerData] = ids.toList.map { id =>
    id -> PeerData(
      PeerMetadata("0.0.0.0", 9000, id, resourceInfo = ResourceInfo(diskUsableBytes = 100L)),
      NonEmptyList.of(MajorityHeight(Some(2L)))
    )
  }.toMap

  before {
    cluster.getPeerInfo shouldReturn peerInfo.pure[IO]
    peerSampling = new RandomPeerSampling(selfId, cluster)
  }

  "generated paths" - {

    "have all the nodes covered" in {
      val paths = peerSampling.selectPaths.unsafeRunSync()
      val flattened = paths.map(_.toIndexedSeq).flatten.filterNot(id => id == selfId).toSet
      flattened.equals(ids) shouldBe true
    }

    "have unique nodes" in {
      val paths = peerSampling.selectPaths.unsafeRunSync()
      val duplicates = paths.map(_.toIndexedSeq).fold(Set.empty[Id]) {
        case (a, b) => a.toSet.intersect(b.toSet).filterNot(id => id == selfId)
      }
      duplicates.size shouldBe 0
    }

    "start and ends with selfId" in {
      val path = peerSampling.selectPaths.unsafeRunSync()

      path.forall(_.toIndexedSeq.head.equals(selfId)) shouldBe true
      path.forall(_.toIndexedSeq.last.equals(selfId)) shouldBe true
    }
  }
}
