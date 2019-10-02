package org.constellation.consensus

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.implicits._
import com.softwaremill.sttp.SttpBackend
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import com.softwaremill.sttp.prometheus.PrometheusBackend
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.{ConstellationExecutionContext, PeerMetadata}
import org.constellation.consensus.Consensus.FacilitatorId
import org.constellation.primitives.Schema.SignedObservationEdge
import org.constellation.domain.schema.Id
import org.constellation.primitives.{ChannelMessage, Observation, TipSoe, Transaction}
import org.constellation.storage.SnapshotService
import org.json4s.native
import org.json4s.native.Serialization
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.Future

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

  implicit val backend: SttpBackend[Future, Nothing] =
    PrometheusBackend[Future, Nothing](OkHttpFutureBackend()(ConstellationExecutionContext.unbounded))

  val consensusManager = mock[ConsensusManager[IO]]
  val snapshotService = mock[SnapshotService[IO]]

  val consensusRoute = new ConsensusRoute(consensusManager, snapshotService, backend)

  "participate route  " - {
    val data = RoundDataRemote(
      ConsensusManager.generateRoundId,
      Set.empty[PeerMetadata],
      Set.empty[PeerMetadata],
      FacilitatorId(Id("foo")),
      List.empty[Transaction],
      TipSoe(Seq.empty[SignedObservationEdge], 2L.some),
      Seq.empty[ChannelMessage],
      List.empty[Observation]
    )
    snapshotService.getNextHeightInterval shouldReturnF 4

    "return error when snapshot height is above tip" in {
      Post("/" + ConsensusRoute.newRoundPath, data) ~> consensusRoute.createBlockBuildingRoundRoutes() ~> check {
        status shouldEqual StatusCodes.BadRequest
      }
    }
  }
}
