package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client.NodeMetadataClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.http4s.client.Client
import io.circe.generic.auto._
import org.constellation.PeerMetadata
import org.constellation.primitives.Schema.AddressCacheData
import org.constellation.util.NodeStateInfo
import org.http4s.circe.CirceEntityDecoder._

class NodeMetadataClientInterpreter[F[_]: Concurrent: ContextShift](client: Client[F])
    extends NodeMetadataClientAlgebra[F] {

  def getNodeState(): PeerResponse[F, NodeStateInfo] =
    PeerResponse[F, NodeStateInfo]("state")(client)

  def getAddressBalance(address: String): PeerResponse[F, Option[AddressCacheData]] =
    PeerResponse[F, Option[AddressCacheData]](s"address/${address}")(client)

  def getPeers(): PeerResponse[F, Seq[PeerMetadata]] =
    PeerResponse[F, Seq[PeerMetadata]]("peers")(client)
}

object NodeMetadataClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](client: Client[F]): NodeMetadataClientInterpreter[F] =
    new NodeMetadataClientInterpreter[F](client)
}
