package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import io.circe.KeyDecoder
import org.constellation.domain.p2p.client.ClusterClientAlgebra
import org.constellation.domain.trust.TrustData
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.p2p.Cluster.ClusterNode
import org.constellation.p2p.{JoinedHeight, PeerUnregister, SetNodeStatus}
import org.http4s.client.Client
import io.circe.generic.auto._
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.Id
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._

class ClusterClientInterpreter[F[_]: ContextShift](client: Client[F])(implicit F: Concurrent[F])
    extends ClusterClientAlgebra[F] {

  implicit val idDecoder: KeyDecoder[Id] = KeyDecoder.decodeKeyString.map(Id)

  def getInfo(): PeerResponse[F, List[ClusterNode]] =
    PeerResponse[F, List[ClusterNode]]("cluster/info")(client)

  def setNodeStatus(status: SetNodeStatus): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean]("status", client, POST) { (req, c) =>
      c.successful(req.withEntity(status))
    }.flatMapF(a => if (a) F.unit else F.raiseError(new Throwable("Cannot set node status")))

  def checkPeerResponsiveness(id: Id): PeerResponse[F, PeerHealthCheck.PeerHealthCheckStatus] =
    PeerResponse[F, PeerHealthCheck.PeerHealthCheckStatus](s"peer-responsiveness/$id")(client)

  def setJoiningHeight(height: JoinedHeight): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean]("joinedHeight", client, POST) { (req, c) =>
      c.successful(req.withEntity(height))
    }.flatMapF(a => if (a) F.unit else F.raiseError(new Throwable("Cannot set joining height")))

  def deregister(peerUnregister: PeerUnregister): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean]("deregister", client, POST) { (req, c) =>
      c.successful(req.withEntity(peerUnregister))
    }.flatMapF(a => if (a) F.unit else F.raiseError(new Throwable("Cannot deregister")))

  def getTrust(): PeerResponse[F, TrustData] =
    PeerResponse[F, TrustData]("trust")(client)
}

object ClusterClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](client: Client[F]): ClusterClientInterpreter[F] =
    new ClusterClientInterpreter[F](client)
}
