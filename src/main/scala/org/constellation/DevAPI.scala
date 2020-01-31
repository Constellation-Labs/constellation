package org.constellation

import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.Directives.{path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.domain.trust.TrustData
import org.constellation.p2p.{Download, SetStateResult}
import org.constellation.primitives.Genesis
import org.constellation.primitives.Schema.{GenesisObservation, NodeState}
import org.constellation.util.{APIDirective, AccountBalance, HostPort}
import org.json4s.native.Serialization

import scala.util.{Failure, Success}

trait DevAPI extends Json4sSupport with StrictLogging {

  implicit val serialization: Serialization.type
  implicit val stringUnmarshaller: FromEntityUnmarshaller[String]
  implicit val dao: DAO

  val devGetEndpoints: Route =
    get {
      path("snapshotInfo") {
        val downloadedMajority = Download.getMajoritySnapshotTest()(dao, ConstellationExecutionContext.bounded)
        val io = downloadedMajority.get

        APIDirective.handle(io)(complete(_))
      } ~
        path("messageService" / Segment) { channelId =>
          APIDirective.handle(dao.messageService.lookup(channelId))(complete(_))
        } ~
        path("messages") {
          APIDirective.handle(IO {
            dao.channelStorage.getLastNMessages(20)
          })(complete(_))
        } ~
        path("messages" / Segment) { channelId =>
          APIDirective.handle(IO {
            dao.channelStorage.getLastNMessages(20, Some(channelId))
          })(complete(_))
        } ~
        path("storedReputation") {
          APIDirective.handle(
            dao.trustManager.getStoredReputation.map(TrustData)
          )(complete(_))
        } ~
        path("predictedReputation") {
          APIDirective.handle(
            dao.trustManager.getPredictedReputation.map(TrustData)
          )(complete(_))
        }
    }

  val devPostEndpoints: Route =
    post {
      path("ready") {
        def changeStateToReady: IO[SetStateResult] = dao.cluster.compareAndSet(NodeState.initial, NodeState.Ready)

        APIDirective.onHandle(changeStateToReady) {
          case Success(SetStateResult(_, true))  => complete(StatusCodes.OK)
          case Success(SetStateResult(_, false)) => complete(StatusCodes.Conflict)
          case Failure(error) =>
            complete(StatusCodes.InternalServerError)
        }
      } ~
        pathPrefix("genesis") {
          path("create") {
            entity(as[Seq[AccountBalance]]) { balances =>
              val go = Genesis.createGenesisObservation(balances)
              complete(go)
            }
          } ~
            path("accept") {
              entity(as[GenesisObservation]) { go =>
                Genesis.acceptGenesis(go, setAsTips = true)
                // TODO: Report errors and add validity check
                complete(StatusCodes.OK)
              }
            }
        } ~
        path("addPeer") {
          entity(as[PeerMetadata]) { pm =>
            (IO
              .contextShift(ConstellationExecutionContext.bounded)
              .shift >> dao.cluster.addPeerMetadata(pm)).unsafeRunAsyncAndForget

            complete(StatusCodes.OK)
          }
        } ~
        pathPrefix("peer") {
          path("add") {
            entity(as[HostPort]) { hp =>
              APIDirective.handle(
                dao.cluster
                  .attemptRegisterPeer(hp)
                  .map(result => {
                    logger.debug(s"Add Peer Request: $hp. Result: $result")
                    StatusCode.int2StatusCode(result.code)
                  })
              )(complete(_))
            }
          }
        }
    }
}
