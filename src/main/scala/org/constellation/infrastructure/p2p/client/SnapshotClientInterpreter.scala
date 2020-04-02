package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import io.circe.{Decoder, KeyDecoder}
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.http4s.client.Client
import io.circe.generic.auto._
import org.constellation.domain.observation.ObservationEvent
import org.constellation.domain.p2p.client.SnapshotClientAlgebra
import org.constellation.domain.redownload.RedownloadService.{
  LatestMajorityHeight,
  SnapshotProposalsAtHeight,
  SnapshotsAtHeight
}
import org.constellation.schema.Id
import org.http4s.{EntityDecoder, MediaType}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._

import scala.collection.SortedMap

class SnapshotClientInterpreter[F[_]: Concurrent: ContextShift](client: Client[F]) extends SnapshotClientAlgebra[F] {

  implicit val idDecoder: KeyDecoder[Id] = KeyDecoder.decodeKeyString.map(Id)
  implicit val smDecoder: Decoder[SortedMap[Id, Double]] =
    Decoder.decodeMap[Id, Double].map(m => SortedMap(m.toSeq: _*))

  def getStoredSnapshots(): PeerResponse[F, List[String]] =
    PeerResponse[F, List[String]]("snapshot/stored")(client)

  def getStoredSnapshot(hash: String): PeerResponse[F, Array[Byte]] =
    PeerResponse[F, Vector[Byte]](s"snapshot/stored/$hash", client, GET) { (req, c) =>
      c.get(req.uri)(_.body.compile.toVector)
    }.map(_.toArray)

  def getCreatedSnapshots(): PeerResponse[F, SnapshotProposalsAtHeight] =
    PeerResponse[F, SnapshotProposalsAtHeight]("snapshot/created")(client)

  def getAcceptedSnapshots(): PeerResponse[F, SnapshotsAtHeight] =
    PeerResponse[F, SnapshotsAtHeight]("snapshot/accepted")(client)

  def getNextSnapshotHeight(): PeerResponse[F, (Id, Long)] =
    PeerResponse[F, (Id, Long)]("snapshot/nextHeight")(client)

  def getSnapshotInfo(): PeerResponse[F, Array[Byte]] = // TODO: 45s timeout
    PeerResponse[F, Vector[Byte]](s"snapshot/info", client, GET) { (req, c) =>
      c.get(req.uri)(_.body.compile.toVector)
    }.map(_.toArray)

  def getSnapshotInfo(hash: String): PeerResponse[F, Array[Byte]] =
    PeerResponse[F, Vector[Byte]](s"snapshot/info/$hash", client, GET) { (req, c) =>
      c.get(req.uri)(_.body.compile.toVector)
    }.map(_.toArray)

  def getLatestMajorityHeight(): PeerResponse[F, LatestMajorityHeight] =
    PeerResponse[F, LatestMajorityHeight](s"latestMajorityHeight")(client)

}

object SnapshotClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](client: Client[F]): SnapshotClientInterpreter[F] =
    new SnapshotClientInterpreter[F](client)
}
