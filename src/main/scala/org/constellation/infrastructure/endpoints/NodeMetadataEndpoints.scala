package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.implicits._
import io.circe.generic.auto._
import io.circe.syntax._
import org.constellation.p2p.Cluster
import org.constellation.primitives.Schema.NodeType
import org.constellation.storage.AddressService
import org.constellation.util.NodeStateInfo
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class NodeMetadataEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def publicEndpoints(addressService: AddressService[F]) = getAddressBalance(addressService)

  def peerEndpoints(
    cluster: Cluster[F],
    addressService: AddressService[F],
    addresses: Seq[String],
    nodeType: NodeType
  ) =
    getNodeState(cluster, addresses, nodeType) <+> getAddressBalance(addressService) <+> getNodePeers(cluster)

  private def getNodeState(cluster: Cluster[F], addresses: Seq[String], nodeType: NodeType): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "state" =>
        cluster.getNodeState.map(NodeStateInfo(_, addresses, nodeType)).map(_.asJson).flatMap(Ok(_))
    }

  private def getAddressBalance(addressService: AddressService[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "address" / address =>
      addressService.lookup(address).map(_.asJson).flatMap(Ok(_))
  }

  private def getNodePeers(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "peers" =>
      cluster.getPeerInfo.map(_.map(_._2.peerMetadata).toSeq).map(_.asJson).flatMap(Ok(_))
  }

  def ownerEndpoints(
    cluster: Cluster[F],
    addressService: AddressService[F],
    addresses: Seq[String],
    nodeType: NodeType
  ) =
    getNodeState(cluster, addresses, nodeType) <+> getAddressBalance(addressService) <+> getNodePeers(cluster)

}

object NodeMetadataEndpoints {

  def publicEndpoints[F[_]: Concurrent](addressService: AddressService[F]): HttpRoutes[F] =
    new NodeMetadataEndpoints[F]().publicEndpoints(addressService)

  def peerEndpoints[F[_]: Concurrent](
    cluster: Cluster[F],
    addressService: AddressService[F],
    addresses: Seq[String],
    nodeType: NodeType
  ): HttpRoutes[F] =
    new NodeMetadataEndpoints[F]().peerEndpoints(cluster, addressService, addresses, nodeType)

  def ownerEndpoints[F[_]: Concurrent](
    cluster: Cluster[F],
    addressService: AddressService[F],
    addresses: Seq[String],
    nodeType: NodeType
  ): HttpRoutes[F] =
    new NodeMetadataEndpoints[F]().ownerEndpoints(cluster, addressService, addresses, nodeType)
}
