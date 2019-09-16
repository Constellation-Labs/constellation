package org.constellation.p2p

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.consensus.Consensus.RoundId
import org.constellation.primitives.Schema.{CheckpointCache, SignedObservationEdgeCache}
import org.constellation.primitives.{ChannelMessageMetadata, Observation, TransactionCacheData}
import org.constellation.storage.ConsensusStatus
import org.constellation.util.{Distance, PeerApiClient}
import org.constellation.util.Logging._

import scala.concurrent.duration._

class DataResolver extends StrictLogging {

  def resolveMessageDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[ChannelMessageMetadata] =
    getPeersForResolving(dao).flatMap(resolveMessage(hash, _, priorityClient)(contextToReturn))

  def resolveMessage(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[ChannelMessageMetadata] =
    logThread(
      resolveDataByDistance[ChannelMessageMetadata](
        List(hash),
        "message",
        pool,
        priorityClient
      )(contextToReturn).head
        .flatTap(mcd => dao.messageService.memPool.put(mcd.channelMessage.signedMessageData.hash, mcd))
        .flatTap(m => IO.delay(logger.debug(s"Resolving message=${m.channelMessage.signedMessageData.hash}"))),
      s"dataResolver_resolveMessage [$hash]",
      logger
    )

  def resolveTransactionDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None,
    roundId: Option[RoundId] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[TransactionCacheData] =
    getPeersForResolving(dao).flatMap(resolveTransaction(hash, _, priorityClient, roundId)(contextToReturn))

  def resolveTransaction(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient],
    roundId: Option[RoundId] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[TransactionCacheData] =
    logThread(
      IO.delay(logger.debug(s"Start resolving transaction=${hash} for round $roundId")) *>
        resolveDataByDistance[TransactionCacheData](
          List(hash),
          "transaction",
          pool,
          priorityClient
        )(contextToReturn).sequence.flatMap { txs =>
          txs.headOption match {
            case Some(tcd) =>
              dao.transactionService
                .put(tcd, ConsensusStatus.Unknown)
                .flatTap(_ => IO.delay(logger.debug(s"Stored resolved transaction=${tcd.hash} for roundId=${roundId}")))
            case _ =>
              IO.raiseError[TransactionCacheData](
                new Throwable(s"Failed with resolving transaction=${hash} for roundId=${roundId}")
              )
          }
        },
      s"dataResolver_resolveTransaction [${hash}]",
      logger
    )

  def resolveCheckpointDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[CheckpointCache] =
    getPeersForResolving(dao).flatMap(resolveCheckpoint(hash, _, priorityClient)(contextToReturn))

  def resolveCheckpoint(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[CheckpointCache] =
    logThread(
      resolveDataByDistance[CheckpointCache](
        List(hash),
        "checkpoint",
        pool,
        priorityClient
      )(contextToReturn).head
        .flatTap(
          cpc =>
            cpc.checkpointBlock.get.storeSOE() *> dao.checkpointService.memPool
              .put(cpc.checkpointBlock.get.baseHash, cpc)
        )
        .flatTap(
          cb =>
            IO.delay(logger.debug(s"Resolving checkpoint=$hash with baseHash=${cb.checkpointBlock.map(_.baseHash)}"))
        ),
      s"dataResolver_resolveCheckpoint [${hash}]",
      logger
    )

  def resolveSoeDefaults(
    hashes: List[String],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[SignedObservationEdgeCache]] =
    getPeersForResolving(dao).flatMap(resolveSoe(hashes, _, priorityClient)(contextToReturn))

  def resolveSoe(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[SignedObservationEdgeCache]] =
    logThread(
      resolveDataByDistanceFlat[SignedObservationEdgeCache](
        hashes,
        "soe",
        pool,
        priorityClient
      )(contextToReturn).flatTap(s => s.traverse(soc => dao.soeService.put(soc.signedObservationEdge.hash, soc))),
      s"dataResolver_resolveSoe [${hashes}]",
      logger
    )

  def resolveCheckpoints(
    hashes: List[String],
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[List[CheckpointCache]] =
    logThread(
      resolveDataByDistanceFlat[CheckpointCache](
        hashes,
        "checkpoint",
        pool,
        priorityClient
      )(contextToReturn).flatTap(
        cbs =>
          cbs.traverse(
            cb =>
              cb.checkpointBlock.get.storeSOE() *> dao.checkpointService.memPool
                .put(cb.checkpointBlock.get.baseHash, cb)
        )
      ),
      s"dataResolver_resolveCheckpoints [${hashes}]",
      logger
    )

  def resolveObservationDefaults(
    hash: String,
    priorityClient: Option[PeerApiClient] = None
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Observation] =
    getPeersForResolving(dao).flatMap(resolveObservation(hash, _, priorityClient)(contextToReturn))

  def resolveObservation(
    hash: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient]
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, dao: DAO): IO[Observation] =
    logThread(
      resolveDataByDistance[Observation](
        List(hash),
        "observation",
        pool,
        priorityClient
      )(contextToReturn).head
        .flatTap(o => IO.delay(logger.debug(s"Resolving observation=${o.hash}")))
        .flatTap(o => dao.observationService.put(o, ConsensusStatus.Unknown)),
      s"dataResolver_resolveObservation [${hash}]",
      logger
    )

  def resolveDataByDistance[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): List[IO[T]] =
    hashes.map { hash =>
      resolveData[T](hash, endpoint, priorityClient.toList ++ pool.sortBy { p =>
        Distance.calculate(hash, p.id)
      })(contextToReturn)
    }

  private[p2p] def resolveData[T <: AnyRef](
    hash: String,
    endpoint: String,
    sortedPeers: List[PeerApiClient],
    maxErrors: Int = 31
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[T] = {

    def makeAttempt(
      innerPeers: Iterable[PeerApiClient],
      allPeers: Iterable[PeerApiClient],
      errorsSoFar: Int = 0
    ): IO[T] =
      innerPeers match {
        case _ if errorsSoFar >= maxErrors =>
          IO.raiseError[T](DataResolutionMaxErrors(endpoint, hash))
        case Nil =>
          IO.raiseError[T](
            DataResolutionOutOfPeers(dao.id.short, endpoint, hash, allPeers.map(_.id.short))
          )
        case head :: tail =>
          getData[T](hash, endpoint, head)(contextToReturn).flatMap {
            case Some(a) => IO.pure(a)
            case None =>
              IO.raiseError[T](
                new Throwable(
                  s"Failed to resolve hash=${hash} on endpoint $endpoint with host=${head.client.hostPortForLogging}, returned None, trying next peer"
                )
              )
          }.handleErrorWith {
            case e: DataResolutionMaxErrors  => IO.raiseError[T](e)
            case e: DataResolutionOutOfPeers => IO.raiseError[T](e)
            case e if tail.isEmpty           => IO.raiseError[T](e)
            case e =>
              logger.error(
                s"Failed to resolve hash=${hash} with host=${head.client.hostPortForLogging}, trying next peer",
                e
              )
              makeAttempt(tail, allPeers, errorsSoFar + 1)
          }

      }

    for {
      _ <- IO.delay(logger.debug(s"Resolve $endpoint/$hash"))
      t <- makeAttempt(sortedPeers, sortedPeers)
    } yield t
  }

  private[p2p] def getData[T <: AnyRef](
    hash: String,
    endpoint: String,
    peerApiClient: PeerApiClient
  )(contextToReturn: ContextShift[IO])(implicit apiTimeout: Duration, m: Manifest[T], dao: DAO): IO[Option[T]] =
    peerApiClient.client
      .getNonBlockingIO[Option[T]](s"$endpoint/$hash", timeout = apiTimeout)(contextToReturn)

  private[p2p] def resolveDataByDistanceFlat[T <: AnyRef](
    hashes: List[String],
    endpoint: String,
    pool: List[PeerApiClient],
    priorityClient: Option[PeerApiClient] = None
  )(
    contextToReturn: ContextShift[IO]
  )(implicit apiTimeout: Duration = 3.seconds, m: Manifest[T], dao: DAO): IO[List[T]] =
    resolveDataByDistance[T](hashes, endpoint, pool, priorityClient)(contextToReturn).sequence

  private[p2p] def getPeersForResolving(dao: DAO): IO[List[PeerApiClient]] = {
    val peers = for {
      ready <- dao.readyPeers
      leaving <- dao.leavingPeers
    } yield (ready ++ leaving)
    peers.map(_.map(p => PeerApiClient(p._1, p._2.client)).toList)
  }

}

object DataResolver extends DataResolver

case class DataResolutionOutOfPeers(thisNode: String, endpoint: String, hash: String, peers: Iterable[String])
    extends Exception(
      s"node [$thisNode] Run out of peers when resolving: $endpoint with hash: $hash following tried: $peers"
    )

case class DataResolutionMaxErrors(endpoint: String, hash: String)
    extends Exception(
      s"Max errors threshold reached when resolving: $endpoint and hash: $hash aborting"
    )
