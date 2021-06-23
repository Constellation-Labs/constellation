package org.constellation.infrastructure.endpoints

import cats.effect.Concurrent
import cats.syntax.all._
import io.circe.{Encoder, KeyEncoder}
import io.circe.syntax._
import io.circe.generic.semiauto._
import org.constellation.p2p.{Cluster, MajorityHeight}
import org.constellation.schema.{Id, NodeState, NodeType}
import org.constellation.storage.AddressService
import org.constellation.util.NodeStateInfo
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import NodeStateInfo._
import org.constellation.domain.cluster.{ClusterStorageAlgebra, NodeStorageAlgebra}

class NodeMetadataEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  def publicEndpoints(
    addressService: AddressService[F],
    nodeStorage: NodeStorageAlgebra[F],
    clusterStorage: ClusterStorageAlgebra[F],
    nodeType: NodeType
  ) =
    getNodeState(nodeStorage, nodeType) <+>
      getAddressBalance(addressService) <+>
      getNodePeers(clusterStorage) <+>
      getPeersMajorityHeights(clusterStorage)

  def peerEndpoints(
    nodeStorage: NodeStorageAlgebra[F],
    clusterStorage: ClusterStorageAlgebra[F],
    addressService: AddressService[F],
    nodeType: NodeType
  ) =
    getNodeState(nodeStorage, nodeType) <+>
      getAddressBalance(addressService) <+>
      getNodePeers(clusterStorage)

  private def getNodeState(nodeStorage: NodeStorageAlgebra[F], nodeType: NodeType): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "state" =>
        nodeStorage.getNodeState.map(NodeStateInfo(_, Seq(), nodeType)).map(_.asJson).flatMap(Ok(_))
    }

  private def getAddressBalance(addressService: AddressService[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "address" / address =>
      addressService.lookup(address).map(_.asJson).flatMap(Ok(_))
  }

  private def getNodePeers(clusterStorage: ClusterStorageAlgebra[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "peers" =>
      clusterStorage.getPeers
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

  private def getPeersMajorityHeights(clusterStorage: ClusterStorageAlgebra[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "peers" / "majority-height" =>
      clusterStorage.getPeers
        .map(_.mapValues(_.majorityHeight.toList))
        .map(_.asJson)
        .flatMap(Ok(_))
  }

  def ownerEndpoints(
    nodeStorage: NodeStorageAlgebra[F],
    clusterStorage: ClusterStorageAlgebra[F],
    addressService: AddressService[F],
    nodeType: NodeType
  ) =
    getNodeState(nodeStorage, nodeType) <+>
      getAddressBalance(addressService) <+>
      getNodePeers(clusterStorage) <+>
      getPeersMajorityHeights(clusterStorage)

}

object NodeMetadataEndpoints {

  def publicEndpoints[F[_]: Concurrent](
    addressService: AddressService[F],
    nodeStorage: NodeStorageAlgebra[F],
    clusterStorage: ClusterStorageAlgebra[F],
    nodeType: NodeType
  ): HttpRoutes[F] =
    new NodeMetadataEndpoints[F]().publicEndpoints(addressService, nodeStorage, clusterStorage, nodeType)

  def peerEndpoints[F[_]: Concurrent](
    nodeStorage: NodeStorageAlgebra[F],
    clusterStorage: ClusterStorageAlgebra[F],
    addressService: AddressService[F],
    nodeType: NodeType
  ): HttpRoutes[F] =
    new NodeMetadataEndpoints[F]().peerEndpoints(nodeStorage, clusterStorage, addressService, nodeType)

  def ownerEndpoints[F[_]: Concurrent](
    nodeStorage: NodeStorageAlgebra[F],
    clusterStorage: ClusterStorageAlgebra[F],
    addressService: AddressService[F],
    nodeType: NodeType
  ): HttpRoutes[F] =
    new NodeMetadataEndpoints[F]().ownerEndpoints(nodeStorage, clusterStorage, addressService, nodeType)
}
