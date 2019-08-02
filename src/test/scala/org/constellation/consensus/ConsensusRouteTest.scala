package org.constellation.consensus

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.implicits._
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.PeerMetadata
import org.constellation.consensus.Consensus.FacilitatorId
import org.constellation.primitives.Schema.{Id, SignedObservationEdge}
import org.constellation.primitives.{ChannelMessage, TipSoe, Transaction}
import org.constellation.storage.SnapshotService
import org.json4s.native
import org.json4s.native.Serialization
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class ConsensusRouteTest
    extends FreeSpec
    with Matchers
    with ScalatestRouteTest
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar
    with Json4sSupport
    with BeforeAndAfter {

  implicit val serialization: Serialization.type = native.Serialization
  implicit val s: ActorSystem = system

  val consensusManager = mock[ConsensusManager[IO]]
  val snapshotService = mock[SnapshotService[IO]]

//  implicit val concurrent = ConstellationConcurrentEffect.global

  val consensusRoute = new ConsensusRoute(consensusManager, snapshotService)

  "participate route  " - {
    val data = RoundDataRemote(
      ConsensusManager.generateRoundId,
      Set.empty[PeerMetadata],
      Set.empty[PeerMetadata],
      FacilitatorId(Id("foo")),
      List.empty[Transaction],
      TipSoe(Seq.empty[SignedObservationEdge], 2L.some),
      Seq.empty[ChannelMessage]
    )
    snapshotService.getLastSnapshotHeight shouldReturnF 2

    "return error when snapshot height is above tip" in {
      Post("/" + ConsensusRoute.newRoundPath, data) ~> consensusRoute.createBlockBuildingRoundRoutes() ~> check {
        status shouldEqual StatusCodes.BadRequest
      }
    }
  }
}
