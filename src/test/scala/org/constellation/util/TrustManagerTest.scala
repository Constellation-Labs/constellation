package org.constellation.util
import cats.effect.{ContextShift, IO}
import org.constellation.{ConstellationExecutionContext, Fixtures}
import org.constellation.TestHelpers.mock
import org.constellation.domain.trust.TrustDataInternal
import org.constellation.p2p.Cluster
import org.constellation.schema.Id
import org.constellation.trust.TrustManager
import org.scalatest.FlatSpec

import scala.util.Try

class TrustManagerTest extends FlatSpec {
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  val id = Fixtures.id
  val cluster = mock[Cluster[IO]]
  val trustManager = TrustManager[IO](id, cluster)

  val peerTrustScoreNewlyJoinedPeer = TrustDataInternal(Fixtures.id2, Map[Id, Double](Fixtures.id -> 1d, Fixtures.id1 -> 1d, Fixtures.id3 -> 1d))
  val peerTrustScores: List[TrustDataInternal] =
    TrustDataInternal(Fixtures.id1, Map[Id, Double](Fixtures.id2 -> 1d, Fixtures.id3 -> 1d)) ::
      peerTrustScoreNewlyJoinedPeer ::
      Nil
  val idMap = Map[Id, Int](Fixtures.id2 -> 1, Fixtures.id -> 2, Fixtures.id3 -> 3)

  "calculateTrustNodes" should "not find key of peer not registered " in {
    val res = Try{trustManager.calculateTrustNodes(peerTrustScores, id, idMap)}
    assert(res.isFailure)
  }

  "calculateIdxMaps" should "include unregistered peers in scoringMap and idxMap" in {
    val (scoringMap, idxMap) = trustManager.calculateIdxMaps(peerTrustScores)
    val res = Try{trustManager.calculateTrustNodes(peerTrustScores, id, scoringMap)}
    assert(res.isSuccess)
  }
}
