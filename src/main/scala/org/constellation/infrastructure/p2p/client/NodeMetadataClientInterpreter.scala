package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.PeerMetadata
import org.constellation.domain.p2p.client.NodeMetadataClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.address.AddressCacheData
import org.constellation.session.SessionTokenService
import org.constellation.util.NodeStateInfo
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client

import scala.language.reflectiveCalls

class NodeMetadataClientInterpreter[F[_]: Concurrent: ContextShift](
  client: Client[F],
  sessionTokenService: SessionTokenService[F]
) extends NodeMetadataClientAlgebra[F] {

  def getNodeState(): PeerResponse[F, NodeStateInfo] =
    PeerResponse[F, NodeStateInfo]("state")(client)

  def getAddressBalance(address: String): PeerResponse[F, Option[AddressCacheData]] =
    PeerResponse[F, Option[AddressCacheData]](s"address/${address}")(client, sessionTokenService)

  def getPeers(): PeerResponse[F, Seq[PeerMetadata]] =
    PeerResponse[F, Seq[PeerMetadata]]("peers")(client, sessionTokenService)
}

object NodeMetadataClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): NodeMetadataClientInterpreter[F] =
    new NodeMetadataClientInterpreter[F](client, sessionTokenService)
}
