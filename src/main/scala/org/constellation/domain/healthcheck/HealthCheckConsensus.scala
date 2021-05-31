package org.constellation.domain.healthcheck

import cats.{Order, Parallel}
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Fiber}
import cats.effect.Concurrent.parTraverseN
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.circe.{Codec, Decoder, Encoder}
import io.circe.generic.semiauto.{deriveCodec, deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.domain.healthcheck.HealthCheckConsensus.{
  CheckedPeer,
  ConsensusHealthStatus,
  HealthcheckRoundId,
  SendConsensusHealthStatus
}
import org.constellation.domain.healthcheck.HealthCheckConsensus.SendConsensusHealthStatus._
import org.constellation.domain.healthcheck.HealthCheckConsensusManagerBase._
import org.constellation.domain.healthcheck.HealthCheckKey.{MissingProposalHealthCheckKey, PingHealthCheckKey}
import org.constellation.domain.healthcheck.HealthCheckLoggingHelper._
import org.constellation.domain.healthcheck.HealthCheckStatus.{
  MissingProposalHealthCheckStatus,
  PeerPingHealthCheckStatus
}
import org.constellation.infrastructure.p2p.{ClientInterpreter, PeerResponse}
import org.constellation.p2p.PeerData
import org.constellation.schema.{Id, NodeState}
import org.constellation.schema.consensus.RoundId

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration, SECONDS}
import scala.util.Random

class HealthCheckConsensus[
  F[_],
  K <: HealthCheckKey: Encoder: Decoder,
  A <: HealthCheckStatus: Encoder: Decoder,
  B <: ConsensusHealthStatus[K, A]: Encoder: Decoder,
  C <: SendConsensusHealthStatus[K, A, B]: Encoder: Decoder
](
  key: K,
  checkedPeer: CheckedPeer,
  ownId: Id,
  roundId: HealthcheckRoundId,
  initialRoundPeers: Map[Id, PeerData],
  delayedHealthCheckStatus: Fiber[F, A],
  startedAtSecond: FiniteDuration,
  val roundType: HealthcheckActiveRoundType,
  metrics: PrefixedHealthCheckMetrics[F],
  apiClient: ClientInterpreter[F],
  unboundedHealthBlocker: Blocker,
  healthHttpPort: String,
  driver: HealthCheckConsensusTypeDriver[K, A, B],
  healthCheckType: HealthCheckType
)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F], C: Clock[F]) {
  import HealthCheckConsensus._

  val logger: PrefixedHealthCheckLogger[F] = new PrefixedHealthCheckLogger[F](healthCheckType)
  val fetchProposalAfter: FiniteDuration = 60.seconds
  val removeUnresponsivePeersWithParallelRoundAfter: FiniteDuration = 120.seconds

  private val roundIds: Ref[F, NonEmptySet[HealthcheckRoundId]] = Ref.unsafe(NonEmptySet.one(roundId))
  private val roundPeers: Ref[F, Map[Id, RoundData[K, A]]] =
    Ref.unsafe(
      initialRoundPeers.mapValues(pd => RoundData(pd.asRight[ProxyPeers]))
    )
  private val removedPeers: Ref[F, Map[Id, NonEmptyList[PeerRemovalReason]]] = Ref.unsafe(Map.empty)
  private val parallelRounds: Ref[F, Map[K, Set[HealthcheckRoundId]]] = Ref.unsafe(Map.empty)

  private val sendingProposalLock = createSemaphore()
  private val fetchingProposalLock = createSemaphore()

  def getTimeInSeconds(): F[FiniteDuration] = C.monotonic(SECONDS).map(_.seconds)

  def start(): F[Fiber[F, Unit]] = F.start {
    roundType match {
      case OwnRound | PeerRound => sendProposal()
    }
  }

  private def getOwnHealthCheckStatus(): F[A] = delayedHealthCheckStatus.join

  def getOwnConsensusHealthStatus(): F[B] =
    for {
      healthCheckStatus <- getOwnHealthCheckStatus()
      consensusHealthStatus = driver.getOwnConsensusHealthStatus(
        key,
        healthCheckStatus,
        checkedPeer,
        ownId,
        roundId,
        initialRoundPeers.mapValues(_.peerMetadata.nodeState)
      )
    } yield consensusHealthStatus

  // TODO: consider if this shouldn't only send the proposal after all, if we mark it as sent and request doesn't arrive
  //       and there is next healthcheck after that we may send the proposal to the asking node and mix two healthchecks
  //       into one
  def getOwnProposalAndMarkAsSent(originId: Id): F[B] =
    for {
      proposal <- getOwnConsensusHealthStatus()
      _ <- markProposalAsSent(Set(originId))
    } yield proposal

  def getRemovedPeers(): F[Map[Id, NonEmptyList[PeerRemovalReason]]] =
    removedPeers.get

  def addRoundIds(ids: Set[HealthcheckRoundId]): F[Unit] =
    roundIds.modify(rounds => (NonEmptySet(rounds.head, rounds.tail ++ ids), ()))

  def getRoundIds(): F[NonEmptySet[HealthcheckRoundId]] = roundIds.get

  def getParallelRounds(): F[Map[K, Set[HealthcheckRoundId]]] = parallelRounds.get

  def addParallelRounds(key: K, roundIds: Set[HealthcheckRoundId]): F[Unit] = parallelRounds.modify { rounds =>
    val updatedIdRounds = rounds.get(key).map(r => r ++ roundIds).getOrElse(roundIds)

    (rounds + (key -> updatedIdRounds), ())
  }

  def getRoundPeers(): F[Map[Id, RoundData[K, A]]] = roundPeers.get

  private def addPeerToRemoved(id: Id, removalReason: PeerRemovalReason): F[Unit] =
    for {
      _ <- metrics.incrementMetricAsync(s"peerRemovedFromConsensus_$removalReason")
      _ <- removedPeers.modify { alreadyRemoved =>
        val updatedRemovalReasons =
          alreadyRemoved
            .get(id)
            .map(_ :+ removalReason)
            .getOrElse(NonEmptyList.one(removalReason))

        (alreadyRemoved + (id -> updatedRemovalReasons), ())
      }
    } yield ()

  private def removeRoundPeer(id: Id, removalReason: PeerRemovalReason): F[Unit] =
    for {
      roundIds <- getRoundIds()
      _ <- logger.debug(
        s"Removing peer with id=${logId(id)} because of removalReason=$removalReason for consensus with roundIds=${logRoundIds(roundIds)}"
      )
      _ <- roundPeers.modify(peers => (peers - id, ()))
      _ <- addPeerToRemoved(id, removalReason)
    } yield ()

  def manageAbsentPeers(absentPeers: Set[Id]): F[Unit] =
    for {
      peers <- getRoundPeers()
      roundIds <- getRoundIds()
      _ <- logger.debug(
        s"Detected absent peers: ${logIds(absentPeers)}. In round with ids=${logRoundIds(roundIds)}"
      )
      // should we only remove peers that didn't finish or all of them
      unsuccessfulPeers = peers.filter {
        case (_, roundData) => !roundData.receivedProposal || roundData.healthStatus.isEmpty
      }.keySet
      toRemove = unsuccessfulPeers.intersect(absentPeers)
      _ <- logger.debug(
        s"Absent peers that were picked for removal: ${logIds(toRemove)}. roundIds=${logRoundIds(roundIds)}"
      )
      _ <- toRemove.toList.traverse(removeRoundPeer(_, LeftDuringConsensus))
    } yield ()

  def manageProxiedPeers(peersThatNoLongerNeedProxying: Map[Id, PeerData]): F[Unit] =
    for {
      _ <- logger.debug(
        s"Detected peers that no longer need proxying, peers=${logIds(peersThatNoLongerNeedProxying.keySet)}."
      )
      _ <- roundPeers.modify { roundPeers =>
        val updatedPeers = roundPeers.filterKeys(peersThatNoLongerNeedProxying.keySet.contains).map {
          case (id, roundData) => id -> roundData.copy(peerData = peersThatNoLongerNeedProxying(id).asRight[ProxyPeers])
        }

        (roundPeers ++ updatedPeers, ())
      }
    } yield ()

  def removeIdAsAProxy(proxyingId: Id, proxyToId: Id): F[Unit] =
    for {
      _ <- logger.debug(s"Proxying id ${logId(proxyingId)} to be removed from proxyPeers list of ${logId(proxyToId)}.")
      maybeRemoved <- roundPeers.modify { peers =>
        val updatedPeer = peers.get(proxyToId).flatMap { roundData =>
          val newPeerData = roundData.peerData match {
            case Left(proxyPeers) =>
              val updatedProxyPeersData = proxyPeers.peersData.filterNot(_.peerMetadata.id == proxyingId)
              if (updatedProxyPeersData.isEmpty)
                None
              else
                ProxyPeers(NonEmptySet(updatedProxyPeersData.head, updatedProxyPeersData.tail)).asLeft[PeerData].some

            case Right(_) => roundData.peerData.some
          }

          newPeerData.map(pd => roundData.copy(peerData = pd))
        }

        updatedPeer match {
          case Some(roundData) => (peers + (proxyToId -> roundData), None)
          case None            => (peers - proxyToId, proxyToId.some)
        }
      }
      _ <- maybeRemoved match {
        case Some(_) => addPeerToRemoved(proxyToId, LostReferenceThroughProxy)
        case None    => F.unit
      }
    } yield ()

  def removeUnresponsivePeersWithParallelRound(unresponsivePeers: Set[Id]): F[Unit] =
    for {
      parallelRounds <- getParallelRounds()
      toRemove = parallelRounds.keySet.map(_.id).intersect(unresponsivePeers)
      _ <- logger.debug(s"Unresponsive peers to be removed ${logIds(toRemove)}.")
      _ <- toRemove.toList.traverse(removeRoundPeer(_, UnresponsiveWithParallelRound))
    } yield ()

  def getCheckedPeersIps(): F[Option[NonEmptyList[String]]] =
    for {
      ips <- getRoundPeers().map(
        _.values.collect {
          case RoundData(_, Some(healthStatus), _) => healthStatus.checkedPeer.ip
        }.collect {
          case Some(ip) => ip
        }.filter(_.nonEmpty)
          .toList
          .groupBy(identity)
          .mapValues(_.size)
          .toList
          .sortBy { case (_, size) => size }
          .reverse
          .map { case (ip, _) => ip }
      )
    } yield NonEmptyList.fromList(ips)

  def runManagementTasks(): F[Unit] =
    for {
      currentTime <- getTimeInSeconds()
      missingPeersData <- getRoundPeers().map(_.filter {
        case (id, data) => data.healthStatus.isEmpty || !data.receivedProposal
      })
      roundIds <- getRoundIds()
      _ <- logger.debug(
        s"Missing peers data while checking key=${logHealthCheckKey(key)} for roundIds=${logRoundIds(roundIds)}: ${logMissingRoundDataForId(missingPeersData)}"
      )
      _ <- if (driver.removePeersWithParallelRound && currentTime - startedAtSecond > removeUnresponsivePeersWithParallelRoundAfter && missingPeersData.keySet.nonEmpty)
        removeUnresponsivePeersWithParallelRound(missingPeersData.keySet)
      else
        F.unit
      _ <- sendProposal() // F.start(...)???
      _ <- if (currentTime - startedAtSecond > fetchProposalAfter) fetchProposals() else F.unit // F.start(...)???
    } yield ()

  def generateHistoricalRoundData(
    healthcheckDecision: HealthcheckConsensusDecision[K]
  ): F[HistoricalRoundData[K, A, B]] =
    for {
      finishedAtSecond <- getTimeInSeconds()
      roundIds <- getRoundIds()
      roundPeers <- getRoundPeers()
      parallelRounds <- getParallelRounds()
      removedPeers <- getRemovedPeers()
      ownConsensusHealthStatus <- getOwnConsensusHealthStatus()
      historicalRound = HistoricalRoundData[K, A, B](
        key = key,
        checkedPeer = checkedPeer,
        startedAtSecond = startedAtSecond,
        finishedAtSecond = finishedAtSecond,
        roundIds = roundIds.toSortedSet,
        decision = healthcheckDecision,
        roundPeers = roundPeers,
        parallelRounds = parallelRounds,
        removedPeers = removedPeers,
        ownConsensusHealthStatus = ownConsensusHealthStatus,
        roundType = HistoricalRound(roundType)
      )
    } yield historicalRound

  def processProposal(healthProposal: ConsensusHealthStatus[K, A]): F[Either[SendProposalError, Unit]] =
    for {
      rIds <- getRoundIds()
      result <- roundPeers.modify(processProposalOrFailWithAnError(healthProposal, rIds.toSortedSet, _))

      _ <- result match {
        //TODO: consider that we could also log the error on the server side in addition to the client side
        case Left(_) => F.unit
        case Right(_) =>
          roundIds.modify { roundIds => //just adding blindly seems better then checking and conditionally modifying
            (roundIds.add(healthProposal.roundId), ())
          }
      }
    } yield result

  private def processProposalOrFailWithAnError(
    healthProposal: ConsensusHealthStatus[K, A],
    roundIds: Set[HealthcheckRoundId],
    roundPeers: Map[Id, RoundData[K, A]]
  ): (Map[Id, RoundData[K, A]], Either[SendProposalError, Unit]) = {
    lazy val initiallyUnknownPeers = healthProposal.clusterState.keySet
      .diff(initialRoundPeers.keySet)
      .filterNot(_ == ownId)

    val result = roundPeers.get(healthProposal.checkingPeerId) match {
      case None =>
        IdNotPartOfTheConsensus(healthProposal.checkingPeerId, Set(healthProposal.roundId), roundType)
          .asLeft[Map[Id, RoundData[K, A]]]

      case Some(RoundData(_, Some(consensusHealthStatus), _)) if consensusHealthStatus == healthProposal =>
        ProposalAlreadyProcessed(healthProposal.roundId, roundType).asLeft[Map[Id, RoundData[K, A]]]

      case Some(RoundData(_, Some(_), _)) if roundIds.contains(healthProposal.roundId) =>
        DifferentProposalForRoundIdAlreadyProcessed(healthProposal.checkingPeerId, roundIds, roundType)
          .asLeft[Map[Id, RoundData[K, A]]]

      case Some(RoundData(_, Some(_), _)) =>
        DifferentProposalAlreadyProcessedForCheckedId(
          checkedPeer.id,
          healthProposal.checkingPeerId,
          roundIds,
          roundType
        ).asLeft[Map[Id, RoundData[K, A]]]

      case Some(rd @ RoundData(_, None, _)) =>
        val updatedPeers = rd.peerData match {
          case Left(_) =>
            roundPeers //for now we don't allow proxying through more than one node - so we don't add unknown peers from this peer
          case Right(peerData) =>
            val addedPeers = (initiallyUnknownPeers -- roundPeers.keySet)
              .map(_ -> RoundData[K, A](ProxyPeers(NonEmptySet.one(peerData)).asLeft[PeerData]))
            val updatedProxyPeers = roundPeers
              .filterKeys(initiallyUnknownPeers.contains)
              .mapValues(
                roundData =>
                  roundData.copy(
                    peerData = roundData.peerData.leftMap(pp => pp.copy(pp.peersData.add(peerData)))
                  )
              )

            roundPeers ++ updatedProxyPeers ++ addedPeers
        }

        (updatedPeers + (healthProposal.checkingPeerId -> rd.copy(healthStatus = healthProposal.some)))
          .asRight[SendProposalError]
    }

    result match {
      case Left(e) =>
        (roundPeers, e.asLeft[Unit])
      case Right(updated) =>
        (updated, ().asRight[SendProposalError])
    }
  }

  private def findPossibleProxyPeers(proxyForId: Id, peers: Map[Id, RoundData[K, A]]): Option[NonEmptySet[PeerData]] = {
    val proxyPeers = (peers - proxyForId).values
      .filter(_.healthStatus.exists(_.clusterState.contains(proxyForId)))
      .map(_.peerData)
      .collect { case Right(peerData) => peerData }
      .toList

    NonEmptySet.fromSet(SortedSet(proxyPeers: _*))
  }

  private def fetchProposals(): F[Unit] =
    for {
      isAcquired <- fetchingProposalLock.tryAcquire
      _ <- if (isAcquired)
        fetchHealthProposals().handleErrorWith { e =>
          logger.error(e)("Error fetching healthcheck proposals.")
        } >> fetchingProposalLock.release
      else
        F.unit
    } yield ()

  private def fetchProposalThroughProxy(
    targetId: Id,
    roundIds: NonEmptySet[HealthcheckRoundId],
    proxyPeers: ProxyPeers
  ): F[Either[FetchProposalError, C]] =
    for {
      proxyPeer <- randomlyPickProxyPeer(proxyPeers.peersData)
      _ <- logger.debug(
        s"Fetching ${logId(targetId)}'s proposal by proxying through ${logId(proxyPeer)} for consensus with roundIds=${logRoundIds(roundIds)}"
      )
      response <- PeerResponse.run(
        apiClient.healthcheck.fetchPeerHealthStatus[C](
          FetchPeerHealthStatus(healthCheckType, roundIds.toSortedSet, ownId, targetId.some)
        ),
        unboundedHealthBlocker
      )(proxyPeer.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort))
    } yield response

  private def fetchProposalDirectly(
    id: Id,
    roundIds: NonEmptySet[HealthcheckRoundId],
    peerData: PeerData
  ): F[Either[FetchProposalError, C]] =
    for {
      _ <- logger.debug(
        s"Fetching ${logId(id)}'s proposal directly for consensus with roundIds=${logRoundIds(roundIds)}"
      )
      response <- PeerResponse.run(
        apiClient.healthcheck
          .fetchPeerHealthStatus[C](FetchPeerHealthStatus(healthCheckType, roundIds.toSortedSet, ownId)),
        unboundedHealthBlocker
      )(peerData.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort))
    } yield response

  private def fetchProposalFromPeer(
    id: Id,
    roundIds: NonEmptySet[HealthcheckRoundId],
    peerData: Either[ProxyPeers, PeerData]
  ): F[Either[(Id, FetchProposalError), (Id, C)]] = {
    peerData match {
      case Left(proxyPeers) =>
        fetchProposalThroughProxy(id, roundIds, proxyPeers)
      case Right(peerData) =>
        fetchProposalDirectly(id, roundIds, peerData).handleErrorWith { error =>
          for {
            _ <- logger.debug(error)(
              s"Error fetching proposal directly from ${logId(id)}. Trying to find a proxy peer and to fetch through proxy. roundIds=${logRoundIds(roundIds)}"
            )
            mostRecentRoundPeers <- roundPeers.get
            possibleProxyPeers = findPossibleProxyPeers(id, mostRecentRoundPeers)
            response <- possibleProxyPeers match {
              case Some(proxyPeers) =>
                logger.debug(
                  s"Found at least one proxy peer for id=${logId(id)}, proxy ids=${logIds(
                    proxyPeers.toSortedSet.map(_.peerMetadata.id)
                  )}. Trying to fetch proposal through proxy. roundIds=${logRoundIds(roundIds)}"
                ) >>
                  fetchProposalThroughProxy(id, roundIds, ProxyPeers(proxyPeers))
              case None =>
                logger.debug(s"Proxy peer for id=${logId(id)} not found! roundIds=${logRoundIds(roundIds)}") >>
                  FetchingFailed(id).asInstanceOf[FetchProposalError].asLeft[C].pure[F]
            }
          } yield response
        }
    }
  }.handleErrorWith(
      e =>
        logger.debug(e)("Error fetching proposal.") >>
          FetchingFailed(id).asInstanceOf[FetchProposalError].asLeft[C].pure[F]
    )
    .map(_.bimap(id -> _, id -> _))

  private def fetchHealthProposals(): F[Unit] =
    for {
      allPeers <- roundPeers.get
      peers = allPeers.filter { case (_, rd) => rd.healthStatus.isEmpty }
      roundIds <- roundIds.get
      responses <- parTraverseN(2)(peers.toList) {
        case (id, roundData) =>
          fetchProposalFromPeer(id, roundIds, roundData.peerData)
      }
      (failed, succeeded) = responses.separate
      _ <- succeeded.traverse {
        case (id, proposal) =>
          logger.debug(
            s"Successfully fetched proposal from peer with id=${logId(id)} proposal=${logConsensusHealthStatus(proposal.consensusHealthStatus)}. roundIds=${logRoundIds(roundIds)}"
          ) >>
            processProposal(proposal.consensusHealthStatus).flatMap {
              case Left(error) => logSendProposalError(error)(metrics, logger)
              case Right(_)    => F.unit
            }.handleErrorWith(
              e =>
                logger.debug(e)(
                  s"Error processing a proposal after fetching from ${logId(id)} proposal=${logConsensusHealthStatus(proposal.consensusHealthStatus)}. roundIds=${logRoundIds(roundIds)}"
                ) >> F.unit
            )
      }
      _ <- failed.traverse {
        case (id, error) =>
          logger.debug(error)(
            s"Error during fetching healthcheck proposal from peer with id=${logId(id)}. roundIds=${logRoundIds(roundIds)}"
          ) >> {
            // TODO: reconsider what is the correct logic for handling fetching errors
            error match {
              // We could remove peer from the round but I think that sending the proposal again with a check that it
              // was sent previously is better. The possible case (the possibility is very small but it's still there)
              // where this could happen is if the node would somehow manage to leave and rejoin the cluster during
              // healthcheck round runtime and we wouldn't catch it. The worst case scenario is that if it joined
              // processing a different round for that id and we send it our proposal from this one we will mix the
              // two rounds together and it will make our node not participate in this new round with other peers.
              // But that can happen even now if the sending triggers first and we will mix those rounds
              // together either way. We could also consider removing the node if we check that it got our proposal
              // but now it is unaware of that rounds existence. Maybe that's the better solution but I'm not sure
              // if it doesn't have its own cases and maybe it will cause mass removal of the peers from the consensus.
              // We could also add a check to the function inspecting round peers to check if the peer's token didn't
              // change in the meantime - we would need to store it along side peerData in the consensus.
              // If so it should be removed. In other cases (that I don't know if they exist) we will just try to
              // send the proposal again. If it works I guess it's fine. Though reconsider this logic.
              case NoConsensusForGivenRounds(_) =>
                for {
                  maybePeerRoundData <- getRoundPeers().map(_.get(id))
                  healthStatus <- getOwnConsensusHealthStatus()
                  _ <- maybePeerRoundData match {
                    case Some(roundData) if roundData.receivedProposal =>
                      logger.warn(
                        s"Resending proposal to ${logId(id)} for round with roundIds=${logRoundIds(roundIds)}"
                      ) >>
                        incrementSuspiciousHealthcheckWarning(metrics) >>
                        sendProposalToPeer(id, healthStatus, roundIds, roundData.peerData)
                          .map(_ => ())
                    case _ => F.unit
                  }
                } yield ()
              // it won't recover - we need to remove the peer
              case MoreThanOneConsensusForGivenRounds(_) | HistoricalAndInProgressRoundMatchedForGivenRounds(_) =>
                removeRoundPeer(id, PeerCouldNotProvideProposal)
              case UnknownProxyIdFetching(proxyToId, proxyingId) =>
                removeIdAsAProxy(proxyingId = proxyingId, proxyToId = proxyToId)
              case ProxyGetFailed(_) | FetchingFailed(_) =>
                F.unit
            }
          }
      }
    } yield ()

  private def randomlyPickProxyPeer(proxyPeers: NonEmptySet[PeerData]): F[PeerData] = F.delay(
    Random.shuffle(proxyPeers.toList).head
  )

  // TODO: create ADT with types indicating state of acceptance e.g. (Accepted, Pending, Discarded) or similar
  //  to not mark it with Boolean and hence have more info about the state
  private def markProposalAsSent(peersThatReceivedProposal: Set[Id]): F[Unit] =
    roundPeers.modify { roundPeers =>
      val updatedPeers = roundPeers
        .filterKeys(peersThatReceivedProposal.contains)
        .mapValues(_.copy(receivedProposal = true))

      (roundPeers ++ updatedPeers, ())
    }

  private def sendProposal(): F[Unit] =
    for {
      isAcquired <- sendingProposalLock.tryAcquire
      _ <- if (isAcquired)
        sendHealthStatus().handleErrorWith { e =>
          logger.error(e)("Error sending healthcheck proposals.")
        } >> sendingProposalLock.release
      else
        F.unit
    } yield ()

  private def sendProposalThroughProxy(
    healthStatus: B,
    targetId: Id,
    roundIds: NonEmptySet[HealthcheckRoundId],
    proxyPeers: ProxyPeers
  ): F[Either[SendProposalError, Unit]] =
    for {
      proxyPeer <- randomlyPickProxyPeer(proxyPeers.peersData)
      _ <- logger.debug(
        s"Sending own health proposal to ${logId(targetId)} by proxying through ${logId(proxyPeer)} for consensus with roundIds=${logRoundIds(roundIds)}"
      )
      response <- PeerResponse.run(
        apiClient.healthcheck.sendPeerHealthStatus(SendConsensusHealthStatus[K, A, B](healthStatus, targetId.some)),
        unboundedHealthBlocker
      )(proxyPeer.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort))
    } yield response

  private def sendProposalDirectly(
    healthStatus: B,
    peerData: PeerData
  ): F[Either[SendProposalError, Unit]] =
    PeerResponse.run(
      apiClient.healthcheck.sendPeerHealthStatus(SendConsensusHealthStatus[K, A, B](healthStatus)),
      unboundedHealthBlocker
    )(peerData.peerMetadata.toPeerClientMetadata.copy(port = healthHttpPort))

  private def sendProposalToPeer(
    id: Id,
    healthStatus: B,
    roundIds: NonEmptySet[HealthcheckRoundId],
    peerData: Either[ProxyPeers, PeerData]
  ): F[Either[(Id, SendProposalError), Id]] = {
    peerData match {
      case Left(proxyPeers) =>
        sendProposalThroughProxy(healthStatus, id, roundIds, proxyPeers)
      case Right(peerData) =>
        sendProposalDirectly(healthStatus, peerData).handleErrorWith { error =>
          for {
            _ <- logger.debug(error)(
              s"Error sending proposal directly to ${logId(id)}. Trying to find a proxy peer and to send through proxy. roundIds=${logRoundIds(roundIds)}"
            )
            mostRecentRoundPeers <- roundPeers.get
            possibleProxyPeers = findPossibleProxyPeers(id, mostRecentRoundPeers)
            response <- possibleProxyPeers match {
              case Some(proxyPeers) =>
                logger.debug(
                  s"Found proxy peer for id=${logId(id)}. Trying to send proposal through proxy. roundIds=${logRoundIds(roundIds)}"
                ) >>
                  sendProposalThroughProxy(healthStatus, id, roundIds, ProxyPeers(proxyPeers))
              case None =>
                logger.debug(s"Proxy peer for id=${logId(id)} not found! roundIds=${logRoundIds(roundIds)}") >>
                  SendingFailed(id).asInstanceOf[SendProposalError].asLeft[Unit].pure[F]
            }
          } yield response
        }
    }
  }.handleErrorWith(
      e =>
        logger.debug(e)("Error sending proposal.") >> SendingFailed(id)
          .asInstanceOf[SendProposalError]
          .asLeft[Unit]
          .pure[F]
    )
    .map(_.bimap(id -> _, _ => id))

  private def sendHealthStatus(): F[Unit] =
    for {
      allPeers <- roundPeers.get
      peers = allPeers.filterNot { case (_, rd) => rd.receivedProposal }
      roundIds <- roundIds.get
      healthStatus <- getOwnConsensusHealthStatus()
      responses <- parTraverseN(2)(peers.toList) {
        // TODO: consider marking as sent inside sendProposalToPeer to not wait until all calls are executed
        case (id, roundData) =>
          sendProposalToPeer(id, healthStatus, roundIds, roundData.peerData)
      }
      (failed, succeeded) = responses.separate
      _ <- markProposalAsSent(succeeded.toSet)
      _ <- failed.traverse {
        case (id, error) =>
          logger.debug(error)(
            s"Error during sending healthcheck proposal to peer with id=${logId(id)}. roundIds=${logRoundIds(roundIds)}"
          ) >> {
            // TODO: reconsider what is the correct logic for handling sending proposal errors
            error match {
              case ProposalAlreadyProcessed(_, _) | DifferentProposalForRoundIdAlreadyProcessed(_, _, _) |
                  ProposalNotProcessedForHistoricalRound(_) =>
                markProposalAsSent(Set(id))
              case IdNotPartOfTheConsensus(_, _, roundType) if roundType.isInstanceOf[HistoricalRound] =>
                markProposalAsSent(Set(id))
              case UnknownProxyIdSending(proxyToId, proxyingId) =>
                removeIdAsAProxy(proxyingId = proxyingId, proxyToId = proxyToId)
              case SendingFailed(_) | ProxySendFailed(_) | InternalErrorStartingRound(_) |
                  IdNotPartOfTheConsensus(_, _, _) | DifferentProposalAlreadyProcessedForCheckedId(_, _, _, _) =>
                F.unit
            }
          }
      }
    } yield ()

  def calculateConsensusOutcome(): F[HealthcheckConsensusDecision[K]] =
    for {
      peersData <- getRoundPeers()
      roundIds <- getRoundIds()
      parallelRounds <- getParallelRounds()
      ownHealthStatus <- getOwnHealthCheckStatus()
      removedPeers <- getRemovedPeers()
      result = driver
        .calculateConsensusOutcome(ownId, key, ownHealthStatus, peersData, roundIds, parallelRounds, removedPeers)
    } yield result

  def isProposalSent(rd: List[RoundData[K, A]]): Boolean = rd.forall(_.receivedProposal)
  def areProposalsReceived(rd: List[RoundData[K, A]]): Boolean = rd.forall(_.healthStatus.nonEmpty)

  def isReadyToCalculateOutcome(): F[Boolean] =
    for {
      peers <- roundPeers.get
      peersData = peers.values.toList
      result = isProposalSent(peersData) && areProposalsReceived(peersData)
    } yield result
}

object HealthCheckConsensus {

  def apply[
    F[_]: Concurrent: ContextShift: Parallel: Clock,
    K <: HealthCheckKey: Encoder: Decoder,
    A <: HealthCheckStatus: Encoder: Decoder,
    B <: ConsensusHealthStatus[K, A]: Encoder: Decoder,
    C <: SendConsensusHealthStatus[K, A, B]: Encoder: Decoder
  ](
    key: K,
    checkedPeer: CheckedPeer,
    ownId: Id,
    roundId: HealthcheckRoundId,
    initialRoundPeers: Map[Id, PeerData],
    delayedHealthCheckStatus: Fiber[F, A],
    startedAtSecond: FiniteDuration,
    roundType: HealthcheckActiveRoundType,
    metrics: PrefixedHealthCheckMetrics[F],
    apiClient: ClientInterpreter[F],
    unboundedHealthBlocker: Blocker,
    healthHttpPort: Int,
    driver: HealthCheckConsensusTypeDriver[K, A, B],
    healthCheckType: HealthCheckType
  ): HealthCheckConsensus[F, K, A, B, C] =
    new HealthCheckConsensus(
      key,
      checkedPeer,
      ownId,
      roundId,
      initialRoundPeers,
      delayedHealthCheckStatus,
      startedAtSecond,
      roundType,
      metrics,
      apiClient,
      unboundedHealthBlocker,
      healthHttpPort = healthHttpPort.toString,
      driver = driver,
      healthCheckType = healthCheckType
    )

  implicit val roundIdOrdering: Ordering[HealthcheckRoundId] = Ordering.by[HealthcheckRoundId, String](_.roundId.id)
  implicit val roundIdOrder: Order[HealthcheckRoundId] = Order.fromOrdering
  implicit val peerDataOrdering: Ordering[PeerData] = Ordering.by[PeerData, String](_.peerMetadata.id.hex)
  implicit val peerDataOrder: Order[PeerData] = Order.fromOrdering

  case class CheckedPeer(id: Id, ip: Option[String])

  object CheckedPeer {
    implicit val checkedPeerCodec: Codec[CheckedPeer] = deriveCodec
  }

  sealed trait ConsensusHealthStatus[+K <: HealthCheckKey, +A <: HealthCheckStatus] {
    val key: K
    val checkedPeer: CheckedPeer
    val checkingPeerId: Id
    val roundId: HealthcheckRoundId
    val status: A
    val clusterState: Map[Id, NodeState]
  }

  object ConsensusHealthStatus {
    implicit val consensusHealthStatusEncoder: Encoder[ConsensusHealthStatus[HealthCheckKey, HealthCheckStatus]] =
      Encoder.instance {
        case a: MissingProposalHealthStatus => a.asJson
        case a: PingHealthStatus            => a.asJson
      }

    implicit val consensusHealthStatusDecoder: Decoder[ConsensusHealthStatus[HealthCheckKey, HealthCheckStatus]] =
      List[Decoder[ConsensusHealthStatus[HealthCheckKey, HealthCheckStatus]]](
        Decoder[MissingProposalHealthStatus].widen,
        Decoder[PingHealthStatus].widen
      ).reduceLeft(_.or(_))
  }

  case class PingHealthStatus(
    key: PingHealthCheckKey,
    checkedPeer: CheckedPeer,
    checkingPeerId: Id,
    roundId: HealthcheckRoundId,
    status: PeerPingHealthCheckStatus,
    clusterState: Map[Id, NodeState]
  ) extends ConsensusHealthStatus[PingHealthCheckKey, PeerPingHealthCheckStatus]

  object PingHealthStatus {
    implicit val pingHealthStatusEncoder: Encoder[PingHealthStatus] = deriveEncoder
    implicit val pingHealthStatusDecoder: Decoder[PingHealthStatus] = deriveDecoder
  }

  case class MissingProposalHealthStatus(
    key: MissingProposalHealthCheckKey,
    checkedPeer: CheckedPeer,
    checkingPeerId: Id,
    roundId: HealthcheckRoundId,
    status: MissingProposalHealthCheckStatus,
    clusterState: Map[Id, NodeState]
  ) extends ConsensusHealthStatus[MissingProposalHealthCheckKey, MissingProposalHealthCheckStatus]

  object MissingProposalHealthStatus {
    implicit val missingSnapshotHealthStatusEncoder: Encoder[MissingProposalHealthStatus] = deriveEncoder
    implicit val missingSnapshotHealthStatusDecoder: Decoder[MissingProposalHealthStatus] = deriveDecoder
  }

  sealed trait HealthConsensusCommand

  object HealthConsensusCommand {
    implicit def encodeHealthConsensusCommand: Encoder[HealthConsensusCommand] = Encoder.instance {
      case a @ SendConsensusHealthStatus(_, _) =>
        a.asJson(
          Encoder[SendConsensusHealthStatus[
            HealthCheckKey,
            HealthCheckStatus,
            ConsensusHealthStatus[HealthCheckKey, HealthCheckStatus]
          ]]
        )
    }

    implicit val decodeHealthConsensusCommand: Decoder[HealthConsensusCommand] =
      List[Decoder[HealthConsensusCommand]](
        Decoder[SendConsensusHealthStatus[
          HealthCheckKey,
          HealthCheckStatus,
          ConsensusHealthStatus[HealthCheckKey, HealthCheckStatus]
        ]].widen
      ).reduceLeft(_.or(_))
  }

  case class SendConsensusHealthStatus[K <: HealthCheckKey, A <: HealthCheckStatus, B <: ConsensusHealthStatus[K, A]](
    consensusHealthStatus: B,
    asProxyForId: Option[Id] = None
  ) extends HealthConsensusCommand

  object SendConsensusHealthStatus {
    implicit def sendConsensusHealthStatusDecoder[
      K <: HealthCheckKey: Decoder,
      A <: HealthCheckStatus: Decoder,
      B <: ConsensusHealthStatus[K, A]: Decoder
    ]: Decoder[SendConsensusHealthStatus[K, A, B]] = deriveDecoder
    implicit def sendConsensusHealthStatusEncoder[
      K <: HealthCheckKey: Encoder,
      A <: HealthCheckStatus: Encoder,
      B <: ConsensusHealthStatus[K, A]: Encoder
    ]: Encoder[SendConsensusHealthStatus[K, A, B]] = deriveEncoder
  }

  case class FetchPeerHealthStatus(
    healthCheckType: HealthCheckType,
    roundIds: Set[HealthcheckRoundId],
    originId: Id,
    asProxyForId: Option[Id] = None
  )

  object FetchPeerHealthStatus {
    implicit val fetchPeerHealthStatusCodec: Codec[FetchPeerHealthStatus] = deriveCodec
  }

  case class HealthcheckRoundId(roundId: RoundId, owner: Id)

  object HealthcheckRoundId {
    implicit val healthcheckRoundIdCodec: Codec[HealthcheckRoundId] = deriveCodec
  }

  case class ProxyPeers(peersData: NonEmptySet[PeerData])

  case class RoundData[K <: HealthCheckKey, A <: HealthCheckStatus](
    peerData: Either[ProxyPeers, PeerData],
    healthStatus: Option[ConsensusHealthStatus[K, A]] = None,
    receivedProposal: Boolean = false
  )

  case class RoundOutcome[K <: HealthCheckKey](roundIds: Set[RoundId], outcome: HealthcheckConsensusDecision[K])

  sealed trait HealthcheckConsensusDecision[K <: HealthCheckKey] {
    val key: K
    val allPeers: Set[Id]
    val remainingPeers: Set[Id]
    val removedPeers: Map[Id, NonEmptyList[PeerRemovalReason]]
    val positiveOutcomePercentage: BigDecimal
    val positiveOutcomeSize: BigDecimal
    val negativeOutcomePercentage: BigDecimal
    val negativeOutcomeSize: BigDecimal
    val parallelRounds: Map[K, Set[HealthcheckRoundId]]
    val roundIds: NonEmptySet[HealthcheckRoundId]
  }

  case class NegativeOutcome[K <: HealthCheckKey](
    key: K,
    allPeers: Set[Id],
    remainingPeers: Set[Id],
    removedPeers: Map[Id, NonEmptyList[PeerRemovalReason]],
    positiveOutcomePercentage: BigDecimal,
    positiveOutcomeSize: BigDecimal,
    negativeOutcomePercentage: BigDecimal,
    negativeOutcomeSize: BigDecimal,
    parallelRounds: Map[K, Set[HealthcheckRoundId]],
    roundIds: NonEmptySet[HealthcheckRoundId]
  ) extends HealthcheckConsensusDecision[K]

  case class PositiveOutcome[K <: HealthCheckKey](
    key: K,
    allPeers: Set[Id],
    remainingPeers: Set[Id],
    removedPeers: Map[Id, NonEmptyList[PeerRemovalReason]],
    positiveOutcomePercentage: BigDecimal,
    positiveOutcomeSize: BigDecimal,
    negativeOutcomePercentage: BigDecimal,
    negativeOutcomeSize: BigDecimal,
    parallelRounds: Map[K, Set[HealthcheckRoundId]],
    roundIds: NonEmptySet[HealthcheckRoundId]
  ) extends HealthcheckConsensusDecision[K]

  // Maybe we could introduce the one below to handle abnormal cases if any exist, for the case where node is processing a round that the node wasn't a part of - e.g. a parallel round -
  // the node shouldn't make it's own observation and it should rely only on the observations from other peers, there is a slight possibility remainingPeers will be empty
  // because we are not adding own proposal
//  case class NotConclusive(
//    id: Id,
//    allPeers: Set[Id],
//    remainingPeers: Set[Id],
//    percentage: BigDecimal,
//    parallelRounds: Map[Id, Set[RoundId]],
//    roundIds: NonEmptySet[RoundId],
//    isKeepUpRound: Boolean
//  ) extends HealthcheckConsensusDecision

  sealed trait PeerRemovalReason
  case object LeftDuringConsensus extends PeerRemovalReason
  case object LostReferenceThroughProxy extends PeerRemovalReason
  case object PeerCouldNotProvideProposal extends PeerRemovalReason
  case object UnresponsiveWithParallelRound extends PeerRemovalReason
  case object ParallelRoundDecidedPeerIsOffline extends PeerRemovalReason
}
