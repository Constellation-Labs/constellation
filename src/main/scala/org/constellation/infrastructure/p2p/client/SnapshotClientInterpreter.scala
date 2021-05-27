package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.constellation.domain.p2p.client.SnapshotClientAlgebra
import org.constellation.domain.redownload.RedownloadService.{SnapshotProposalsAtHeight, SnapshotsAtHeight}
import org.constellation.gossip.state.GossipMessage
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.Id
import org.constellation.schema.snapshot.{LatestMajorityHeight, SnapshotProposalPayload}
import org.constellation.session.SessionTokenService
import org.http4s.Method._
import org.http4s.Status.Successful
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client

import scala.collection.SortedMap
import scala.language.reflectiveCalls

class SnapshotClientInterpreter[F[_]: ContextShift](
  client: Client[F],
  sessionTokenService: SessionTokenService[F]
)(implicit F: Concurrent[F])
    extends SnapshotClientAlgebra[F] {

  import Id._

  implicit val smDecoder: Decoder[SortedMap[Id, Double]] =
    Decoder.decodeMap[Id, Double].map(m => SortedMap(m.toSeq: _*))
  implicit val idLongDecoder: Decoder[(Id, Long)] = deriveDecoder[(Id, Long)]

  def getStoredSnapshots(): PeerResponse[F, List[String]] =
    PeerResponse[F, List[String]]("snapshot/stored")(client, sessionTokenService)

  def getStoredSnapshot(hash: String): PeerResponse[F, Array[Byte]] =
    PeerResponse[F, Vector[Byte]](s"snapshot/stored/$hash", GET)(client, sessionTokenService) { (req, c) =>
      c.get(req.uri) {
        case Successful(response) => response.body.compile.toVector
        case response             => F.raiseError(new Throwable(response.status.reason))
      }
    }.map(_.toArray)

  def getCreatedSnapshots(): PeerResponse[F, SnapshotProposalsAtHeight] =
    PeerResponse[F, SnapshotProposalsAtHeight]("snapshot/created")(client, sessionTokenService)

  def getAcceptedSnapshots(): PeerResponse[F, SnapshotsAtHeight] =
    PeerResponse[F, SnapshotsAtHeight]("snapshot/accepted")(client, sessionTokenService)

  def getPeerProposals(id: Id): PeerResponse[F, Option[SnapshotProposalsAtHeight]] =
    PeerResponse[F, Option[SnapshotProposalsAtHeight]](s"peer/${id.hex}/snapshot/created")(
      client,
      sessionTokenService
    )

  def getNextSnapshotHeight(): PeerResponse[F, (Id, Long)] =
    PeerResponse[F, (Id, Long)]("snapshot/nextHeight")(client, sessionTokenService)

  def getSnapshotInfo(): PeerResponse[F, Array[Byte]] = // TODO: 45s timeout
    PeerResponse[F, Vector[Byte]](s"snapshot/info", GET)(client, sessionTokenService) { (req, c) =>
      c.get(req.uri) {
        case Successful(response) => response.body.compile.toVector
        case response             => F.raiseError(new Throwable(response.status.reason))
      }
    }.map(_.toArray)

  def getSnapshotInfo(hash: String): PeerResponse[F, Array[Byte]] =
    PeerResponse[F, Vector[Byte]](s"snapshot/info/$hash", GET)(client, sessionTokenService) { (req, c) =>
      c.get(req.uri) {
        case Successful(response) => response.body.compile.toVector
        case response             => F.raiseError(new Throwable(response.status.reason))
      }
    }.map(_.toArray)

  def getLatestMajorityHeight(): PeerResponse[F, LatestMajorityHeight] =
    PeerResponse[F, LatestMajorityHeight](s"latestMajorityHeight")(client, sessionTokenService)

  def postPeerProposal(message: GossipMessage[SnapshotProposalPayload]): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean](s"peer/snapshot/created", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(message))
    }.flatMapF(
      a =>
        if (a) F.unit
        else
          F.raiseError(
            new Throwable(
              s"Cannot post proposal of hash ${message.payload.proposal.value.hash} and height ${message.payload.proposal.value.height}"
            )
          )
    )
}

object SnapshotClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): SnapshotClientInterpreter[F] =
    new SnapshotClientInterpreter[F](client, sessionTokenService)
}
