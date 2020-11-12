package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.{Encoder, KeyEncoder}
import io.circe.syntax._
import io.circe.generic.semiauto._
import org.constellation.p2p.{Cluster, MajorityHeight}
import org.constellation.schema.v2.{Id, NodeState, NodeType}
import org.constellation.storage.AddressService
import org.constellation.util.NodeStateInfo
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import NodeStateInfo._

class NodeMetadataEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def publicEndpoints(addressService: AddressService[F], cluster: Cluster[F], nodeType: NodeType) =
    getNodeState(cluster, nodeType) <+>
      getAddressBalance(addressService) <+>
      getNodePeers(cluster) <+>
      getPeersMajorityHeights(cluster)

  def peerEndpoints(
    cluster: Cluster[F],
    addressService: AddressService[F],
    nodeType: NodeType
  ) =
    getNodeState(cluster, nodeType) <+>
      getAddressBalance(addressService) <+>
      getNodePeers(cluster)

  private def getNodeState(cluster: Cluster[F], nodeType: NodeType): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "state" =>
        cluster.getNodeState.map(NodeStateInfo(_, Seq(), nodeType)).map(_.asJson).flatMap(Ok(_))
    }

  private def getAddressBalance(addressService: AddressService[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "address" / address =>
      addressService.lookup(address).map(_.asJson).flatMap(Ok(_))
  }

  private def getNodePeers(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "peers" =>
      cluster.getPeerInfo
        .map(
          _.map { case (_, pd) => pd }
            .filter(pd => NodeState.canActAsJoiningSource(pd.peerMetadata.nodeState))
            .map(_.peerMetadata)
            .toSeq
        )
        .map(_.asJson)
        .flatMap(Ok(_))
  }

  import Id._
  implicit val peersMajorityHeightsEncoder: Encoder[Map[Id, List[MajorityHeight]]] =
    Encoder.encodeMap[Id, List[MajorityHeight]]

  private def getPeersMajorityHeights(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "peers" / "majority-height" =>
      cluster.getPeerInfo
        .map(_.mapValues(_.majorityHeight.toList))
        .map(_.asJson)
        .flatMap(Ok(_))
  }

  def ownerEndpoints(
    cluster: Cluster[F],
    addressService: AddressService[F],
    nodeType: NodeType
  ) =
    getNodeState(cluster, nodeType) <+>
      getAddressBalance(addressService) <+>
      getNodePeers(cluster) <+>
      getPeersMajorityHeights(cluster)

}

object NodeMetadataEndpoints {

  def publicEndpoints[F[_]: Concurrent](
    addressService: AddressService[F],
    cluster: Cluster[F],
    nodeType: NodeType
  ): HttpRoutes[F] =
    new NodeMetadataEndpoints[F]().publicEndpoints(addressService, cluster, nodeType)

  def peerEndpoints[F[_]: Concurrent](
    cluster: Cluster[F],
    addressService: AddressService[F],
    nodeType: NodeType
  ): HttpRoutes[F] =
    new NodeMetadataEndpoints[F]().peerEndpoints(cluster, addressService, nodeType)

  def ownerEndpoints[F[_]: Concurrent](
    cluster: Cluster[F],
    addressService: AddressService[F],
    nodeType: NodeType
  ): HttpRoutes[F] =
    new NodeMetadataEndpoints[F]().ownerEndpoints(cluster, addressService, nodeType)
}
