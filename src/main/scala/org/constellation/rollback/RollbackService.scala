package org.constellation.rollback

import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.consensus.{SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Genesis
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.rewards.{RewardSnapshot, RewardsManager}
import org.constellation.storage.SnapshotService
import org.constellation.util.AccountBalances.AccountBalances

class RollbackService[F[_]: Concurrent](
  dao: DAO,
  rollbackBalances: RollbackAccountBalances,
  snapshotService: SnapshotService[F],
  rollbackLoader: RollbackLoader,
  rewardsManager: RewardsManager[F],
)(implicit C: ContextShift[F]) {

  val logger = Slf4jLogger.getLogger[F]

  type RollbackData = (Seq[StoredSnapshot], SnapshotInfo, GenesisObservation)

  def validateAndRestoreFromSnapshotInfoOnly(): EitherT[F, RollbackException, Unit] =
    for {
      snapshotInfo <- EitherT.fromEither[F](rollbackLoader.loadSnapshotInfoFromFile())
      _ <- EitherT.liftF(logger.info("SnapshotInfo file loaded"))
      _ <- EitherT.liftF(acceptSnapshotInfo(snapshotInfo))
      _ <- EitherT.liftF(logger.info("SnapshotInfo restored"))
    } yield ()

  def validateAndRestore(): EitherT[F, RollbackException, Unit] =
    for {
      rollbackData <- validate()
      _ <- restore(rollbackData)
    } yield ()

  def validate(): EitherT[F, RollbackException, RollbackData] =
    for {
      snapshots <- EitherT.fromEither[F](rollbackLoader.loadSnapshotsFromFile())
      _ <- EitherT.liftF(logger.info("Snapshots files loaded"))

      snapshotInfo <- EitherT.fromEither[F](rollbackLoader.loadSnapshotInfoFromFile())
      _ <- EitherT.liftF(logger.info("SnapshotInfo file loaded"))

      genesisObservation <- EitherT.fromEither[F](rollbackLoader.loadGenesisObservation())
      _ <- EitherT.liftF(logger.info("GenesisObservation file loaded"))

      balances <- EitherT.fromEither[F](rollbackBalances.calculate(snapshotInfo.snapshot.lastSnapshot, snapshots))
      genesisBalances <- EitherT.fromEither[F](rollbackBalances.calculate(genesisObservation))
      _ <- EitherT.fromEither[F](validateAccountBalance(balances |+| genesisBalances))
      _ <- EitherT.liftF(logger.info("Account balances validated"))
    } yield (snapshots, snapshotInfo, genesisObservation)

  def restore(rollbackData: RollbackData): EitherT[F, RollbackException, Unit] =
    for {
      _ <- acceptSnapshots(rollbackData._1)
      _ <- EitherT.liftF(logger.info("Snapshots restored on disk"))

      _ <- EitherT.liftF(acceptRewards(rollbackData._1))
      _ <- EitherT.liftF(logger.info("Rewards restored"))

      _ <- EitherT.liftF(acceptGenesis(rollbackData._3))
      _ <- EitherT.liftF(logger.info("GenesisObservation restored"))

      _ <- EitherT.liftF(acceptSnapshotInfo(rollbackData._2))
      _ <- EitherT.liftF(logger.info("SnapshotInfo restored"))
    } yield ()

  private def acceptGenesis(genesisObservation: GenesisObservation): F[Unit] = Sync[F].delay {
    Genesis.acceptGenesis(genesisObservation)(dao)
  }

  private def acceptSnapshotInfo(snapshotInfo: SnapshotInfo): F[Unit] =
    snapshotService.setSnapshot(snapshotInfo)

  private def acceptSnapshots(
    snapshots: Seq[StoredSnapshot]
  )(implicit C: ContextShift[F]): EitherT[F, RollbackException, Unit] =
    snapshots.toList
      .traverse(snapshotService.addSnapshotToDisk)
      .bimap(_ => CannotWriteToDisk, _ => ())

  private def acceptRewards(
    snapshots: Seq[StoredSnapshot]
  ): F[Unit] = {
    val rewardSnapshots = snapshots
      .map(s => {
        val snapshotheight = s.checkpointCache.flatMap(_.height).maxBy(_.max).max
        RewardSnapshot(s.snapshot.hash, snapshotheight, s.checkpointCache.flatMap(_.checkpointBlock.observations))
      })

    rewardSnapshots.toList.traverse(rewardsManager.attemptReward).void
  }

  private def validateAccountBalance(accountBalances: AccountBalances): Either[RollbackException, Unit] =
    accountBalances.count(_._2 < 0) match {
      case 0 => Right(())
      case _ => Left(InvalidBalances)
    }
}
