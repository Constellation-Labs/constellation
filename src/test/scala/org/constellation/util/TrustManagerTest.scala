package org.constellation.util
import cats.effect.{ContextShift, IO}
import org.constellation.domain.trust.TrustDataInternal
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.schema.Id
import org.constellation.trust.{DataGeneration, SelfAvoidingWalk, TrustManager, TrustNode}
import org.constellation.{ConstellationExecutionContext, Fixtures}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.util.Try

class TrustManagerTest
    extends AnyFlatSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private def mockTrustManager: TrustManager[IO] = mock[TrustManager[IO]]

  val cluster: Cluster[IO] = mock[Cluster[IO]]
  cluster.getPeerInfo shouldReturn IO { Map[Id, PeerData](Fixtures.id -> mock[PeerData]) }
  val id = Fixtures.id
  val trustManager = TrustManager[IO](id, cluster)

  val peerTrustScoreNewlyJoinedPeer =
    TrustDataInternal(Fixtures.id2, Map[Id, Double](Fixtures.id -> 1d, Fixtures.id1 -> 1d, Fixtures.id3 -> 1d))

  val peerTrustScores: List[TrustDataInternal] =
    TrustDataInternal(Fixtures.id1, Map[Id, Double](Fixtures.id2 -> 1d, Fixtures.id3 -> 1d)) ::
      peerTrustScoreNewlyJoinedPeer ::
      Nil
  val idMap = Map[Id, Int](Fixtures.id2 -> 1, Fixtures.id -> 2, Fixtures.id3 -> 3)

  "calculateTrustNodes" should "not find key of peer not registered " in {
    val res = Try { TrustManager.calculateTrustNodes(peerTrustScores, id, idMap) }
    assert(res.isFailure)
  }

  "calculateIdxMaps" should "include unregistered peers in scoringMap and idxMap" in {
    val (scoringMap, idxMap) = TrustManager.calculateIdxMaps(peerTrustScores)
    val res = Try { TrustManager.calculateTrustNodes(peerTrustScores, id, scoringMap) }
    assert(res.isSuccess)
  }

  "handleTrustScoreUpdate" should "update trust scores" in {
    val scores = TrustDataInternal(id, Map()) :: Nil
    val res = Try { trustManager.handleTrustScoreUpdate(scores) }
    assert(res.isSuccess)
  }

  "walk" should "handle edges to peers not yet joined or have left local node" in {
    val reputation = Map.empty[Id, Double]
    val scores = peerTrustScores :+ TrustDataInternal(id, reputation)
    val (scoringMap, idxMap) = TrustManager.calculateIdxMaps(scores)
    val dummyTrustNodes = DataGeneration.generateTestData(3)
    val nodeMap = dummyTrustNodes.zipWithIndex.toMap.map { case (k, v) => (v, k) }
    val doWalk = Try { SelfAvoidingWalk.walk(1, 2, nodeMap, 1, 0, Set.empty[Int], 1d) }
    assert(doWalk.isSuccess)
  }
}
