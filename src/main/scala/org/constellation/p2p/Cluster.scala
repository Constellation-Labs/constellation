package org.constellation.p2p

import java.time.LocalDateTime

import cats.data.{EitherT, NonEmptyList, OptionT}
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Concurrent, ContextShift, IO, LiftIO, Timer}
import cats.syntax.all._
import cats.kernel.Order
import constellation._
import enumeratum._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import org.constellation._
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.p2p.Cluster.ClusterNode
import org.constellation.schema.{Id, NodeState, PeerNotification}
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Logging._
import org.constellation.util._

import scala.concurrent.duration._
import scala.util.Random
import io.circe.syntax._
import io.circe.parser._
import org.constellation.domain.redownload.RedownloadService.{LatestMajorityHeight, SnapshotProposalsAtHeight}
import org.constellation.schema.signature.Signable
import org.constellation.session.SessionTokenService
import org.constellation.session.SessionTokenService.Token

case class SetNodeStatus(id: Id, nodeStatus: NodeState)

object SetNodeStatus {
  implicit val setNodeStatusDecoder: Decoder[SetNodeStatus] = deriveDecoder
  implicit val setNodeStatusEncoder: Encoder[SetNodeStatus] = deriveEncoder
}

case class SetStateResult(oldState: NodeState, isNewSet: Boolean)
case class PeerData(
  peerMetadata: PeerMetadata,
  majorityHeight: NonEmptyList[MajorityHeight],
  notification: Seq[PeerNotification] = Seq.empty
) {

  def updateJoiningHeight(height: Long): PeerData =
    this.copy(
      majorityHeight = NonEmptyList.one(majorityHeight.head.copy(joined = Some(height))) ++ majorityHeight.tail
    )

  def updateLeavingHeight(leavingHeight: Long): PeerData = {
    val joinedHeight = majorityHeight.head.joined

    val leavingHeightToSet = if (joinedHeight.exists(_ <= leavingHeight)) Some(leavingHeight) else joinedHeight

    this.copy(
      majorityHeight = NonEmptyList.one(majorityHeight.head.copy(left = leavingHeightToSet)) ++ majorityHeight.tail
    )
  }

  def canBeRemoved(lowestMajorityHeight: Long): Boolean =
    majorityHeight.forall(_.left.exists(_ < lowestMajorityHeight))
}

case class PendingRegistration(ip: String, request: PeerRegistrationRequest)

case class Deregistration(ip: String, port: Int, id: Id)

case class JoinedHeight(id: Id, height: Long)

object JoinedHeight {
  implicit val joinedHeightDecoder: Decoder[JoinedHeight] = deriveDecoder
  implicit val joinedHeightEncoder: Encoder[JoinedHeight] = deriveEncoder
}

case object GetPeerInfo

case class UpdatePeerInfo(peerData: PeerData)
case class ChangePeerState(id: Id, state: NodeState)

case class MajorityHeight(joined: Option[Long], left: Option[Long] = None) {
  def isFinite = joined.isDefined && left.isDefined
}

object MajorityHeight {
  implicit val majorityHeightEncoder: Encoder[MajorityHeight] = deriveEncoder
  implicit val majorityHeightDecoder: Decoder[MajorityHeight] = deriveDecoder

  def genesis: MajorityHeight = MajorityHeight(Some(0L), None)

  def isHeightBetween(height: Long, majorityHeight: MajorityHeight): Boolean =
    majorityHeight.joined.exists(_ < height) && majorityHeight.left.forall(_ >= height)

  def isHeightBetween(height: Long)(majorityHeights: NonEmptyList[MajorityHeight]): Boolean =
    majorityHeights.exists(isHeightBetween(height, _))
}

case class UpdatePeerNotifications(notifications: Seq[PeerNotification])

class Cluster[F[_]](
  joiningPeerValidator: JoiningPeerValidator[F],
  apiClient: ClientInterpreter[F],
  sessionTokenService: SessionTokenService[F],
  dao: DAO,
  unboundedBlocker: Blocker
)(
  implicit F: Concurrent[F],
  C: ContextShift[F],
  T: Timer[F]
) {

  def id = dao.id

  def alias = dao.alias

  private val ownJoinedHeight: Ref[F, Option[Long]] = Ref.unsafe[F, Option[Long]](None)
  private val participatedInGenesisFlow: Ref[F, Option[Boolean]] = Ref.unsafe[F, Option[Boolean]](None)
  private val participatedInRollbackFlow: Ref[F, Option[Boolean]] = Ref.unsafe[F, Option[Boolean]](None)
  private val joinedAsInitialFacilitator: Ref[F, Option[Boolean]] = Ref.unsafe[F, Option[Boolean]](None)

  private val initialState: NodeState =
    if (dao.nodeConfig.cliConfig.startOfflineMode) NodeState.Offline else NodeState.PendingDownload
  private val nodeState: Ref[F, NodeState] = Ref.unsafe[F, NodeState](initialState)
  private val peers: Ref[F, Map[Id, PeerData]] = Ref.unsafe[F, Map[Id, PeerData]](Map.empty)

  private val peerDiscovery = PeerDiscovery[F](apiClient, this, dao.id, unboundedBlocker)

  implicit val logger = Slf4jLogger.getLogger[F]

  implicit val shadedDao: DAO = dao

  dao.metrics.updateMetricAsync[IO]("nodeState", initialState.toString).unsafeRunAsync(_ => ())
  private val stakingAmount = ConfigUtil.getOrElse("constellation.staking-amount", 0L)

  def setJoinedAsInitialFacilitator(joined: Boolean): F[Unit] =
    joinedAsInitialFacilitator.modify { j =>
      (Some(j.getOrElse(joined)), ())
    }

  def setParticipatedInGenesisFlow(participated: Boolean): F[Unit] =
    participatedInGenesisFlow.modify { p =>
      (Some(p.getOrElse(participated)), ())
    }

  def setParticipatedInRollbackFlow(participated: Boolean): F[Unit] =
    participatedInRollbackFlow.modify { p =>
      (Some(p.getOrElse(participated)), ())
    }

  def setOwnJoinedHeight(height: Long): F[Unit] =
    ownJoinedHeight.modify { h =>
      val updated = Some(h.getOrElse(height))
      (updated, updated.get)
    }.flatMap(dao.metrics.updateMetricAsync("cluster_ownJoinedHeight", _))

  def getOwnJoinedHeight(): F[Option[Long]] =
    ownJoinedHeight.get

  def clearOwnJoinedHeight(): F[Unit] =
    ownJoinedHeight.modify { _ =>
      (None, ())
    }

  // TODO: wkoszycki consider using complex structure of peers so the lookup has O(n) complexity
  def getPeerData(host: String): F[Option[PeerData]] =
    peers.get
      .map(m => m.find(t => t._2.peerMetadata.host == host).map(_._2))

  def clusterNodes(): F[List[ClusterNode]] =
    getPeerInfo.map(_.values.toList.map(_.peerMetadata).map(ClusterNode(_))).flatMap { nodes =>
      getNodeState
        .map(ClusterNode(dao.alias.getOrElse(dao.id.short), dao.id, dao.peerHostPort, _, 0L))
        .map(nodes ++ List(_))
    }

  def getPeerInfo: F[Map[Id, PeerData]] = peers.get

  def getNodeState: F[NodeState] = nodeState.get

  def updatePeerNotifications(notifications: List[PeerNotification]): F[Unit] =
    logThread(
      for {
        p <- peers.get
        peerUpdate = notifications.flatMap { n =>
          p.get(n.id).map { p =>
            p.copy(notification = p.notification.diff(Seq(n)))
          }
        }
        _ <- logger.debug(s"Peer update: ${peerUpdate}")
        _ <- peerUpdate.traverse(updatePeerInfo) // TODO: bulk update
      } yield (),
      "cluster_updatePeerNotifications"
    )

  def updateJoinedHeight(joinedHeight: JoinedHeight): F[Unit] = {
    val peerId = joinedHeight.id

    peers.modify { m =>
      m.get(peerId) match {
        case Some(peerData) =>
          (
            m.updated(
              peerId,
              peerData.updateJoiningHeight(joinedHeight.height)
            ),
            ()
          )
        case _ => (m, ())
      }
    }
  }

  def setNodeStatus(id: Id, state: NodeState): F[Unit] =
    logThread(
      for {
        _ <- peers.modify(
          p =>
            (
              p.get(id)
                .map { pd =>
                  p + (id -> pd.copy(peerMetadata = pd.peerMetadata.copy(nodeState = state)))
                }
                .getOrElse(p),
              p
            )
        )

        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_setNodeStatus"
    )

  def pendingRegistration(
    ip: String,
    request: PeerRegistrationRequest,
    withDiscovery: Boolean = true
  ): F[Unit] =
    logThread(
      for {
        pr <- PendingRegistration(ip, request).pure[F]
        _ <- logger.debug(s"Pending Registration request: $pr")

        validExternalHost = validWithLoopbackGuard(request.host)
        peers <- peers.get
        // TODO: Move to JoiningPeerValidator
        existsNotOffline = peers.exists {
          case (_, d) =>
            val existingId = d.peerMetadata.id == request.id
            val existingIP = d.peerMetadata.host == request.host
            val canActAsJoiningSource = NodeState
              .canActAsJoiningSource(
                d.peerMetadata.nodeState
              )
            (existingId || existingIP) && canActAsJoiningSource
        }

        // TODO: Move to JoiningPeerValidator
        isCorrectIP = ip == request.host && ip != ""
        // TODO: Move to JoiningPeerValidator
        isWhitelisted = dao.nodeConfig.whitelisting.contains(request.id)
        whitelistingHash = KryoSerializer.serializeAnyRef(dao.nodeConfig.whitelisting).sha256
        validWhitelistingHash = whitelistingHash == request.whitelistingHash

        isSelfId = dao.id == request.id
        badAttempt = !isCorrectIP || !isWhitelisted || !validWhitelistingHash || isSelfId || !validExternalHost || existsNotOffline
        isValid <- joiningPeerValidator.isValid(
          PeerClientMetadata(
            request.host,
            request.port,
            request.id
          )
        )

        _ <- if (!isWhitelisted) {
          dao.metrics.incrementMetricAsync[F]("notWhitelistedAdditionAttempt")
        } else F.unit

        _ <- if (badAttempt) {
          dao.metrics.incrementMetricAsync[F]("badPeerAdditionAttempt")
        } else if (!isValid) {
          dao.metrics.incrementMetricAsync[F]("invalidPeerAdditionAttempt")
        } else {
          val authSignRequest = PeerAuthSignRequest(Random.nextLong())
          val peerClientMetadata = PeerClientMetadata(
            request.host,
            request.port,
            request.id
          )
          val req = PeerResponse.run(apiClient.sign.sign(authSignRequest), unboundedBlocker)(peerClientMetadata)

          req.flatTap { sig =>
            if (sig.hashSignature.id != request.id) {
              logger.warn(
                s"Keys should be the same: ${sig.hashSignature.id} != ${request.id}"
              ) >>
                dao.metrics.incrementMetricAsync[F]("peerKeyMismatch")
            } else F.unit
          }.flatTap { sig =>
            if (!sig.valid(authSignRequest.salt.toString)) {
              logger.warn(s"Invalid peer signature $request $authSignRequest $sig") >>
                dao.metrics.incrementMetricAsync[F]("invalidPeerRegistrationSignature")
            } else F.unit
          }.flatTap { sig =>
            dao.metrics.incrementMetricAsync[F]("peerAddedFromRegistrationFlow") >>
              logger.debug(s"Valid peer signature $request $authSignRequest $sig")
          }.flatMap {
            sig =>
              val updateJoiningHeight = for {
                _ <- setParticipatedInGenesisFlow(request.participatesInGenesisFlow)
                _ <- setParticipatedInRollbackFlow(request.participatesInRollbackFlow)
                _ <- setJoinedAsInitialFacilitator(request.joinsAsInitialFacilitator)
              } yield ()

              val updateToken =
                sessionTokenService.updatePeerToken(ip, request.token)

              for {
                state <- withMetric(
                  PeerResponse.run(apiClient.nodeMetadata.getNodeState(), unboundedBlocker)(peerClientMetadata),
                  "nodeState"
                )
                id = sig.hashSignature.id
                existingMajorityHeight <- getPeerInfo.map(
                  _.get(id).map(_.majorityHeight).filter(_.head.isFinite)
                )
                alias = dao.nodeConfig.whitelisting.get(id).flatten
                peerMetadata = PeerMetadata(
                  request.host,
                  request.port,
                  id,
                  nodeState = state.nodeState,
                  auxAddresses = state.addresses,
                  resourceInfo = request.resourceInfo,
                  alias = alias
                )
                majorityHeight = MajorityHeight(request.majorityHeight)
                majorityHeights = existingMajorityHeight
                  .map(_.prepend(majorityHeight))
                  .getOrElse(NonEmptyList.one(majorityHeight))
                peerData = PeerData(peerMetadata, majorityHeights)
                _ <- updatePeerInfo(peerData) >> updateJoiningHeight >> updateToken >> C.shift >> F.start(
                  if (withDiscovery) peerDiscovery.discoverFrom(peerMetadata) else F.unit
                )
              } yield ()
          }.handleErrorWith { err =>
            logger.warn(
              s"Sign request to ${request.host}:${request.port} failed. $err"
            ) >>
              dao.metrics.incrementMetricAsync[F]("peerSignatureRequestFailed")
          }
        }
      } yield (),
      "cluster_pendingRegistration"
    )

  def broadcastOwnJoinedHeight(): F[Unit] = {
    val discoverJoinedHeight = for {
      p <- peers.get.map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
      clients = p.map(_._2.peerMetadata).toList
      maxMajorityHeight <- clients
        .traverse(
          pm =>
            PeerResponse
              .run(
                apiClient.snapshot
                  .getLatestMajorityHeight(),
                unboundedBlocker
              )(pm.toPeerClientMetadata)
              .map(_.some)
              .handleError(_ => none[LatestMajorityHeight])
        )
        .map(_.flatten)
        .map(heights => if (heights.nonEmpty) heights.map(_.highest).max else 0L)
      delay = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

      participatedInGenesisFlow <- participatedInGenesisFlow.get.map(_.getOrElse(false))
      participatedInRollbackFlow <- participatedInRollbackFlow.get.map(_.getOrElse(false))
      joinedAsInitialFacilitator <- joinedAsInitialFacilitator.get.map(_.getOrElse(false))

      ownHeight = if (participatedInGenesisFlow && joinedAsInitialFacilitator) 0L
      else if (participatedInRollbackFlow && joinedAsInitialFacilitator) maxMajorityHeight
      else maxMajorityHeight + delay

      _ <- setOwnJoinedHeight(ownHeight)
    } yield ownHeight

    for {
      height <- getOwnJoinedHeight()
      _ <- logger.debug(s"Broadcasting own joined height - step1: height=$height")
      ownHeight <- height.map(_.pure[F]).getOrElse(discoverJoinedHeight)
      _ <- logger.debug(s"Broadcasting own joined height - step2: height=$ownHeight")
      _ <- broadcast(
        PeerResponse.run(apiClient.cluster.setJoiningHeight(JoinedHeight(dao.id, ownHeight)), unboundedBlocker)
      )
      _ <- F.start(
        T.sleep(10.seconds) >> broadcast(
          PeerResponse.run(apiClient.cluster.setJoiningHeight(JoinedHeight(dao.id, ownHeight)), unboundedBlocker)
        )
      )
      _ <- F.start(
        T.sleep(30.seconds) >> broadcast(
          PeerResponse.run(apiClient.cluster.setJoiningHeight(JoinedHeight(dao.id, ownHeight)), unboundedBlocker)
        )
      )
    } yield ()
  }

  def deregister(peerUnregister: PeerUnregister): F[Unit] =
    logThread(
      for {
        p <- peers.get
        _ <- p.get(peerUnregister.id).traverse { peer =>
          updatePeerInfo(
            peer
              .updateLeavingHeight(peerUnregister.majorityHeight)
              .copy(peerMetadata = peer.peerMetadata.copy(nodeState = NodeState.Leaving))
          )
        }
      } yield (),
      "cluster_deregister"
    )

  def updatePeerInfo(peerData: PeerData): F[Unit] =
    logThread(
      for {
        _ <- peers.modify(p => (p + (peerData.peerMetadata.id -> peerData), p))
        ip = peerData.peerMetadata.host
        _ <- logger.debug(s"Added $ip to known peers.")
        _ <- LiftIO[F].liftIO(dao.registerAgent(peerData.peerMetadata.id))
        _ <- updateMetrics()
        _ <- updatePersistentStore()
      } yield (),
      "cluster_updatePeerInfo"
    )

  private def updateMetrics(): F[Unit] =
    peers.get.flatMap { p =>
      dao.metrics.updateMetricAsync[F](
        "peers",
        p.map {
          case (idI, clientI) =>
            val addr = s"http://${clientI.peerMetadata.host}:${clientI.peerMetadata.httpPort - 1}"
            s"${idI.short} API: $addr"
        }.mkString(" --- ")
      )
    }

  private def updatePersistentStore(): F[Unit] =
    peers.get.flatMap { p =>
      F.delay(dao.peersInfoPath.write(p.values.toSeq.map(_.peerMetadata).asJson.noSpaces))
    }

  private def timeoutTo[A](fa: F[A], after: FiniteDuration, fallback: F[A]): F[A] = // Consider extracting
    F.race(T.sleep(after), fa).flatMap {
      case Left(_)  => fallback
      case Right(a) => F.pure(a)
    }

  def markOfflinePeer(nodeId: Id): F[Unit] = logThread(
    {
      implicit val snapOrder: Order[SnapshotProposalsAtHeight] =
        Order.by[SnapshotProposalsAtHeight, List[Long]](a => a.keySet.toList.sorted)

      val leavingFlow: F[Unit] = for {
        p <- peers.get
        notOfflinePeers = p.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) }
        _ <- logger.info(s"Mark offline peer: ${nodeId}")
        leavingPeer <- notOfflinePeers
          .get(nodeId)
          .fold(
            F.raiseError[PeerData](
              new Throwable(s"Peer id=${nodeId} cannot be find in the peers map or can be already offline")
            )
          )(_.pure[F])
        leavingPeerId = leavingPeer.peerMetadata.id
        majorityHeight <- LiftIO[F].liftIO(dao.redownloadService.latestMajorityHeight)

        proposals <- notOfflinePeers.toList.traverse {
          case (_, pd) =>
            timeoutTo(
              PeerResponse
                .run(
                  apiClient.snapshot
                    .getPeerProposals(leavingPeerId),
                  unboundedBlocker
                )(pd.peerMetadata.toPeerClientMetadata)
                .handleErrorWith(
                  _ =>
                    logger.debug(
                      s"Cannot get peer proposals for leaving node (id=${leavingPeerId} host=${leavingPeer.peerMetadata.host}) from peer (id=${pd.peerMetadata.id} host=${pd.peerMetadata.host}). Fallback to empty"
                    ) >> F.pure(None)
                ),
              5.seconds,
              F.pure(none[SnapshotProposalsAtHeight])
            )
        }.flatMap { list =>
          LiftIO[F]
            .liftIO(dao.redownloadService.getPeerProposals().map(_.get(leavingPeerId)))
            .map(list.::)
        }.map(_.flatten)

        maxProposal = proposals.maximumOption
        _ <- maxProposal.map { proposal =>
          logger.debug(s"Maximum proposal for leaving node id=${leavingPeerId} is empty? ${proposal.isEmpty}") >>
            LiftIO[F].liftIO(dao.redownloadService.updatePeerProposals(leavingPeerId, proposal))
        }.getOrElse(F.unit)

        maxProposalHeight = maxProposal.flatMap(_.keySet.toList.maximumOption)
        joiningHeight = leavingPeer.majorityHeight.head.joined

        leavingHeight = maxProposalHeight
          .map(height => if (height > majorityHeight) majorityHeight else height)
          .orElse(joiningHeight)
          .getOrElse(majorityHeight)

        _ <- logger.debug(
          s"Leaving node id=${leavingPeerId} | maxProposalHeight=${maxProposalHeight} | joiningHeight=${joiningHeight} | leavingHeight=${leavingHeight} "
        )

        newPeerData = leavingPeer
          .updateLeavingHeight(leavingHeight)
          .copy(peerMetadata = leavingPeer.peerMetadata.copy(nodeState = NodeState.Offline))

        _ <- peers.modify(p => (p + (leavingPeerId -> newPeerData), p)) >> updateMetrics >> updatePersistentStore

        _ <- sessionTokenService.clearPeerToken(leavingPeer.peerMetadata.host)

        _ <- logger.info(s"Mark offline peer: ${nodeId} - finished")
      } yield ()

      leavingFlow.handleErrorWith { err =>
        logger.error(s"Error during marking peer as offline: ${err.getMessage}") >> F.raiseError(err)
      }
    },
    "cluster_markOfflinePeer"
  )

  def removePeer(pd: PeerData): F[Unit] =
    logThread(
      LiftIO[F]
        .liftIO(dao.redownloadService.lowestMajorityHeight)
        .map(height => pd.canBeRemoved(height))
        .ifM(
          for {
            p <- peers.get
            pm = pd.peerMetadata

            _ <- peers.modify(a => (a - pm.id, a - pm.id))
            // Technically we shouldn't remove it from eigenTrust if we want to keep the trained model for that node
            //        _ <- LiftIO[F].liftIO(dao.eigenTrust.unregisterAgent(pm.id))
            _ <- updateMetrics()
            _ <- updatePersistentStore()
          } yield (),
          F.unit
        ),
      "cluster_removePeer"
    )

  def internalHearthbeat(round: Long) =
    logThread(
      for {
        peers <- peers.get

        _ <- if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
          peers.values.toList.traverse { pd =>
            PeerResponse
              .run(
                apiClient.metrics
                  .checkHealth(),
                unboundedBlocker
              )(pd.peerMetadata.toPeerClientMetadata)
              .flatTap { _ =>
                dao.metrics.incrementMetricAsync[F]("peerHealthCheckPassed")
              }
              .handleErrorWith { _ =>
                dao.metrics.incrementMetricAsync[F]("peerHealthCheckFailed") >>
                  setNodeStatus(pd.peerMetadata.id, NodeState.Offline)
              }
          }.void
        } else F.unit

        _ <- if (round % dao.processingConfig.peerHealthCheckInterval == 0) {
          peers.values.toList.traverse { pd =>
            peerDiscovery.discoverFrom(pd.peerMetadata)
          }.void
        } else F.unit
      } yield (),
      "cluster_internalHearthbeat"
    )

  def initiateRejoin(): F[Unit] =
    logThread(
      for {
        _ <- if (dao.peersInfoPath.nonEmpty) {
          logger.warn(
            "Found existing peers in persistent storage. Node probably crashed before and will try to rejoin the cluster."
          )
          rejoin()
        } else {
          logger.warn("No peers in persistent storage. Skipping rejoin.")
        }
      } yield (),
      "cluster_initiatePeerReload"
    )

  // Someone joins to me
  def pendingRegistrationRequest(isReconciliationJoin: Boolean = false): F[PeerRegistrationRequest] = {
    for {
      height <- getOwnJoinedHeight()

      peers <- peers.get.map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
      peersSize = peers.size
      minFacilitatorsSize = dao.processingConfig.numFacilitatorPeers
      isInitialFacilitator = peersSize < minFacilitatorsSize
      participatedInRollbackFlow <- participatedInRollbackFlow.get.map(_.getOrElse(false))
      participatedInGenesisFlow <- participatedInGenesisFlow.get.map(_.getOrElse(false))
      whitelistingHash = KryoSerializer.serializeAnyRef(dao.nodeConfig.whitelisting).sha256
      oOwnToken <- sessionTokenService.getOwnToken()
      ownToken <- oOwnToken.map(F.pure).getOrElse(F.raiseError(new Throwable("Own token not set!")))

      _ <- logger.debug(
        s"Pending registration request: ownHeight=$height peers=$peersSize isInitialFacilitator=$isInitialFacilitator participatedInRollbackFlow=$participatedInRollbackFlow participatedInGenesisFlow=$participatedInGenesisFlow"
      )

      prr <- F.delay {
        PeerRegistrationRequest(
          dao.externalHostString,
          dao.externalPeerHTTPPort,
          dao.id,
          ResourceInfo(
            diskUsableBytes = new java.io.File(dao.snapshotPath).getUsableSpace
          ),
          height,
          participatesInGenesisFlow = participatedInGenesisFlow,
          participatesInRollbackFlow = participatedInRollbackFlow,
          joinsAsInitialFacilitator = isInitialFacilitator,
          whitelistingHash,
          ownToken,
          isReconciliationJoin
        )
      }
    } yield prr
  }.handleErrorWith { err =>
    logger.error(err)("Creating pendingRegistrationRequest failed!") >>
      err.raiseError[F, PeerRegistrationRequest]
  }

  // I join to someone
  def attemptRegisterPeer(peerClientMetadata: PeerClientMetadata, isReconciliationJoin: Boolean = false): F[Unit] =
    logThread(
      withMetric(
        {
          {
            if (isReconciliationJoin) F.unit
            else sessionTokenService.createAndSetNewOwnToken().map(_ => ())
          } >>
            dao.nodeConfig.whitelisting
              .contains(peerClientMetadata.id)
              .pure[F]
              .ifM(
                PeerResponse
                  .run(
                    apiClient.sign
                      .getRegistrationRequest(),
                    unboundedBlocker
                  )(peerClientMetadata)
                  .flatMap { registrationRequest =>
                    for {
                      _ <- pendingRegistration(peerClientMetadata.host, registrationRequest, !isReconciliationJoin)
                      prr <- pendingRegistrationRequest(isReconciliationJoin)
                      response <- PeerResponse.run(apiClient.sign.register(prr), unboundedBlocker)(peerClientMetadata)
                    } yield response
                  }
                  .handleErrorWith { err =>
                    logger.error(err)("Registration request failed") >>
                      dao.metrics.incrementMetricAsync[F]("peerGetRegistrationRequestFailed") >>
                      err.raiseError[F, Unit]
                  },
                logger.error(s"unknown peer, not whitelisted")
              )

        },
        "addPeerWithRegistrationSymmetric"
      ),
      "cluster_attemptRegisterPeer"
    )

  def rejoin(): F[Unit] =
    //    def attemptRejoin(lastKnownAPIClients: Seq[HostPort]): F[Unit] =
    //      lastKnownAPIClients match {
    //        case Nil => F.raiseError(new RuntimeException("Couldn't rejoin the cluster"))
    //        case hostPort :: remainingHostPorts =>
    //          join(hostPort)
    //            .handleErrorWith(err => {
    //              logger.error(s"Couldn't rejoin via ${hostPort.host}: ${err.getMessage}")
    //              attemptRejoin(remainingHostPorts)
    //            })
    //      }
    for {
      _ <- logger.warn("Trying to rejoin the cluster...")

      lastKnownPeers = parse(dao.peersInfoPath.lines.mkString)
        .map(_.as[Seq[PeerMetadata]])
        .toOption
        .flatMap(_.toOption)
        .getOrElse(Seq.empty[PeerMetadata])
        .map(pm => HostPort(pm.host, pm.httpPort))

      //      _ <- attemptRejoin(lastKnownPeers)
    } yield ()

  private def clearServicesBeforeJoin(): F[Unit] =
    for {
      _ <- clearOwnJoinedHeight()
      _ <- LiftIO[F].liftIO(dao.blacklistedAddresses.clear)
      _ <- LiftIO[F].liftIO(dao.transactionChainService.clear)
      _ <- LiftIO[F].liftIO(dao.addressService.clear)
      _ <- LiftIO[F].liftIO(dao.soeService.clear)
      _ <- LiftIO[F].liftIO(dao.transactionService.clear)
      _ <- LiftIO[F].liftIO(dao.observationService.clear)
      _ <- LiftIO[F].liftIO(dao.redownloadService.clear)
      _ <- sessionTokenService.clear()
      // TODO: add services to clear if needed
    } yield ()

  def join(hp: PeerClientMetadata): F[Unit] =
    logThread(
      {
        for {
          state <- nodeState.get
          _ <- if (NodeState.canJoin(state)) F.unit else F.raiseError(new Throwable("Node attempted to double join"))
          _ <- clearServicesBeforeJoin()
          _ <- attemptRegisterPeer(hp)
          _ <- T.sleep(15.seconds)
          _ <- LiftIO[F].liftIO(dao.downloadService.download())
          _ <- broadcastOwnJoinedHeight()
        } yield ()
      }.handleErrorWith { error =>
        logger.error(error)("Error during joining process! Leaving the cluster!") >>
          leave(
            logger.warn("Left the cluster because of failed joining process!")
          )
      },
      "cluster_join"
    )

  def leave(gracefulShutdown: => F[Unit]): F[Unit] =
    logThread(
      for {
        _ <- logger.info("Trying to gracefully leave the cluster")

        majorityHeight <- LiftIO[F].liftIO(dao.redownloadService.latestMajorityHeight)
        _ <- broadcastLeaveRequest(majorityHeight)
        _ <- compareAndSet(NodeState.all, NodeState.Leaving)

        // TODO: make interval check to wait for last n snapshots, set Offline state only in Leaving see #641 for details
        _ <- Timer[F].sleep(dao.processingConfig.leavingStandbyTimeout.seconds)

        _ <- compareAndSet(NodeState.all, NodeState.Offline)

        _ <- peers.modify(_ => (Map.empty, Map.empty))
        _ <- sessionTokenService.clearOwnToken()
        _ <- if (dao.nodeConfig.isGenesisNode) setOwnJoinedHeight(0L) else clearOwnJoinedHeight()
        _ <- LiftIO[F].liftIO(dao.eigenTrust.clearAgents())
        _ <- updateMetrics()
        _ <- updatePersistentStore()

        _ <- gracefulShutdown
      } yield (),
      "cluster_leave"
    )

  def compareAndSet(expected: Set[NodeState], newState: NodeState, skipBroadcast: Boolean = false): F[SetStateResult] =
    nodeState.modify { current =>
      if (expected.contains(current)) (newState, SetStateResult(current, isNewSet = true))
      else (current, SetStateResult(current, isNewSet = false))
    }.flatTap(
        res =>
          if (res.isNewSet) LiftIO[F].liftIO(dao.metrics.updateMetricAsync[IO]("nodeState", newState.toString))
          else F.unit
      )
      .flatTap {
        case SetStateResult(_, true) if !skipBroadcast && NodeState.broadcastStates.contains(newState) =>
          broadcastNodeState(newState)
        case _ => F.unit
      }

  private def broadcastLeaveRequest(majorityHeight: Long): F[Unit] = {
    def peerUnregister(c: PeerClientMetadata) =
      PeerUnregister(dao.peerHostPort.host, dao.peerHostPort.port, dao.id, majorityHeight)
    broadcast(c => PeerResponse.run(apiClient.cluster.deregister(peerUnregister(c)), unboundedBlocker)(c)).void
  }

  def broadcastOfflineNodeState(nodeId: Id = dao.id): F[Unit] =
    broadcastNodeState(NodeState.Offline, nodeId)

  private def broadcastNodeState(nodeState: NodeState, nodeId: Id = dao.id): F[Unit] =
    logThread(
      broadcast(
        PeerResponse.run(apiClient.cluster.setNodeStatus(SetNodeStatus(nodeId, nodeState)), unboundedBlocker),
        Set(nodeId)
      ).flatTap {
        _.filter(_._2.isLeft).toList.traverse {
          case (id, e) => logger.warn(s"Unable to propagate status to node ID: $id")
        }
      }.void,
      "cluster_broadcastNodeState"
    )

  def broadcast[T](
    f: PeerClientMetadata => F[T],
    skip: Set[Id] = Set.empty,
    subset: Set[Id] = Set.empty
  ): F[Map[Id, Either[Throwable, T]]] =
    logThread(
      for {
        peerInfo <- peers.get.map(_.filter { case (_, pd) => NodeState.isNotOffline(pd.peerMetadata.nodeState) })
        selected = if (subset.nonEmpty) {
          peerInfo.filterKeys(subset.contains)
        } else {
          peerInfo.filterKeys(id => !skip.contains(id))
        }
        (keys, values) = selected.toList.unzip
        res <- values
          .map(_.peerMetadata.toPeerClientMetadata)
          .map(f)
          .traverse { fa =>
            fa.map(_.asRight[Throwable]).handleErrorWith(_.asLeft[T].pure[F])
          }
          .map(v => keys.zip(v).toMap)
      } yield res,
      "cluster_broadcast"
    )

  private def validWithLoopbackGuard(host: String): Boolean =
    (host != dao.externalHostString && host != "127.0.0.1" && host != "localhost") || !dao.preventLocalhostAsPeer

  private def validPeerAddition(hp: HostPort, peerInfo: Map[Id, PeerData]): Boolean = {
    val hostAlreadyExists = peerInfo.exists {
      case (_, data) => data.peerMetadata.host == hp.host && data.peerMetadata.httpPort == hp.port
    }
    validWithLoopbackGuard(hp.host) && !hostAlreadyExists
  }
}

object Cluster {

  type Peers = Map[Id, PeerData]

  def apply[F[_]: Concurrent: Timer: ContextShift](
    metrics: () => Metrics,
    joiningPeerValidator: JoiningPeerValidator[F],
    apiClient: ClientInterpreter[F],
    sessionTokenService: SessionTokenService[F],
    dao: DAO,
    unboundedBlocker: Blocker
  ) = new Cluster(joiningPeerValidator, apiClient, sessionTokenService, dao, unboundedBlocker)

  case class ClusterNode(alias: String, id: Id, ip: HostPort, status: NodeState, reputation: Long)

  object ClusterNode {

    def apply(pm: PeerMetadata) =
      new ClusterNode(pm.alias.getOrElse(pm.id.short), pm.id, HostPort(pm.host, pm.httpPort), pm.nodeState, 0L)

    implicit val clusterNodeEncoder: Encoder[ClusterNode] = deriveEncoder
    implicit val clusterNodeDecoder: Decoder[ClusterNode] = deriveDecoder
  }

}
