package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client.ClusterClientAlgebra
import org.constellation.domain.trust.TrustData
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.p2p.Cluster.ClusterNode
import org.constellation.p2p.{JoinedHeight, PeerUnregister, SetNodeStatus}
import org.http4s.client.Client
import org.constellation.domain.p2p.PeerHealthCheck
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.Id
import org.constellation.session.SessionTokenService
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._
import scala.language.reflectiveCalls

class ClusterClientInterpreter[F[_]: ContextShift](client: Client[F], sessionTokenService: SessionTokenService[F])(
  implicit F: Concurrent[F]
) extends ClusterClientAlgebra[F] {

  import Id._
  import ClusterNode._
  import SetNodeStatus._
  import JoinedHeight._
  import TrustData._

  def getInfo(): PeerResponse[F, List[ClusterNode]] =
    PeerResponse[F, List[ClusterNode]]("cluster/info")(client, sessionTokenService)

  def setNodeStatus(status: SetNodeStatus): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean]("status", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(status))
    }.flatMapF(a => if (a) F.unit else F.raiseError(new Throwable("Cannot set node status")))

  def setJoiningHeight(height: JoinedHeight): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean]("joinedHeight", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(height))
    }.flatMapF(a => if (a) F.unit else F.raiseError(new Throwable("Cannot set joining height")))

  def deregister(peerUnregister: PeerUnregister): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean]("deregister", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req.withEntity(peerUnregister))
    }.flatMapF(a => if (a) F.unit else F.raiseError(new Throwable("Cannot deregister")))

  def getTrust(): PeerResponse[F, TrustData] =
    PeerResponse[F, TrustData]("trust")(client, sessionTokenService)

  def getActiveFullNodes(): PeerResponse[F, Option[Set[Id]]] =
    PeerResponse[F, Option[Set[Id]]]("cluster/active-full-nodes")(client, sessionTokenService)

  def notifyAboutClusterJoin(): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean]("cluster/join-notification", POST)(client, sessionTokenService) { (req, c) =>
      c.successful(req)
    }.flatMapF { isSuccess =>
      if (isSuccess) F.unit
      else F.raiseError(new Throwable("Failed to notify active full node about cluster joining!"))
    }
}

object ClusterClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): ClusterClientInterpreter[F] =
    new ClusterClientInterpreter[F](client, sessionTokenService)
}
