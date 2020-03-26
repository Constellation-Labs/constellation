package org.constellation.rollback

import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.{ConfigUtil, DAO}
import org.constellation.consensus.StoredSnapshot
import org.constellation.domain.cloud.HeightHashFileStorage
import org.constellation.domain.redownload.RedownloadService
import org.constellation.domain.rewards.StoredRewards
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.genesis.{GenesisObservationLocalStorage, GenesisObservationS3Storage}
import org.constellation.p2p.Cluster
import org.constellation.primitives.Genesis
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.rewards.{EigenTrust, RewardsManager}
import org.constellation.storage.SnapshotService
import org.constellation.util.AccountBalances.AccountBalances

case class RollbackData(
  snapshotInfo: SnapshotInfo,
  storedSnapshot: StoredSnapshot,
  genesisObservation: GenesisObservation
)

class RollbackService[F[_]: Concurrent](
  dao: DAO,
  rollbackBalances: RollbackAccountBalances,
  snapshotService: SnapshotService[F],
  rollbackLoader: RollbackLoader,
  rewardsManager: RewardsManager[F],
  eigenTrust: EigenTrust[F],
  snapshotLocalStorage: LocalFileStorage[F, StoredSnapshot],
  snapshotInfoLocalStorage: LocalFileStorage[F, SnapshotInfo],
  snapshotCloudStorage: HeightHashFileStorage[F, StoredSnapshot],
  snapshotInfoCloudStorage: HeightHashFileStorage[F, SnapshotInfo],
  rewardsLocalStorage: LocalFileStorage[F, StoredRewards],
  rewardsCloudStorage: HeightHashFileStorage[F, StoredRewards],
  genesisObservationLocalStorage: GenesisObservationLocalStorage[F],
  genesisObservationCloudStorage: GenesisObservationS3Storage[F],
  redownloadService: RedownloadService[F],
  cluster: Cluster[F]
)(implicit C: ContextShift[F]) {
  private val logger = Slf4jLogger.getLogger[F]
  private val snapshotHeightInterval: Int = ConfigUtil.constellation.getInt("snapshot.snapshotHeightInterval")

  def restore(): EitherT[F, Throwable, Unit] =
    for {
      _ <- logger.debug("Performing rollback by finding the highest snapshot in the cloud.").attemptT
      highest <- getHighest()
      _ <- logger.debug(s"Max height found: $highest").attemptT
      _ <- highest match {
        case (height, hash) => restore(height, hash)
      }
    } yield ()

  def restore(height: Long, hash: String): EitherT[F, Throwable, Unit] =
    validate(height, hash).flatMap(restore(_, height))

  private[rollback] def validate(height: Long, hash: String): EitherT[F, Throwable, RollbackData] =
    for {
      _ <- logger.debug(s"Validating rollback data for height $height and hash $hash").attemptT
      snapshot <- snapshotCloudStorage.read(height, hash)
      snapshotInfo <- snapshotInfoCloudStorage.read(height, hash)
      genesisObservation <- genesisObservationCloudStorage.read()
      addressData = snapshotInfo.addressCacheData.map {
        case (address, data) => (address, data.balance)
      }
      _ <- validateAccountBalance(addressData)
    } yield RollbackData(snapshotInfo, snapshot, genesisObservation)

  private[rollback] def restore(rollbackData: RollbackData, height: Long): EitherT[F, Throwable, Unit] =
    for {
      _ <- logger.debug("Applying the rollback.").attemptT
      _ <- logger.debug(s"Accepting GenesisObservation").attemptT
      _ <- acceptGenesis(rollbackData.genesisObservation)
      _ <- logger.debug(s"Accepting Snapshot").attemptT
      _ <- acceptSnapshot(rollbackData.storedSnapshot, height)
      _ <- logger.debug(s"Accepting SnapshotInfo").attemptT
      _ <- acceptSnapshotInfo(rollbackData.snapshotInfo)
      _ <- logger.debug("Rollback finished succesfully").attemptT
    } yield ()

  private def getHighest(): EitherT[F, Throwable, (Long, String)] =
    for {
      snapshotInfos <- snapshotInfoCloudStorage.list()
      highest = snapshotInfos.map {
        _.split('-') match {
          case Array(height, hash) => (height.toLong, hash)
        }
      }.maxBy { case (height, _) => height }
    } yield highest

  private def acceptGenesis(genesisObservation: GenesisObservation): EitherT[F, Throwable, Unit] =
    Concurrent[F].delay {
      Genesis.acceptGenesis(genesisObservation)(dao)
    }.attemptT

  private def acceptSnapshotInfo(snapshotInfo: SnapshotInfo): EitherT[F, Throwable, Unit] =
    for {
      _ <- snapshotInfoLocalStorage.write(snapshotInfo.snapshot.snapshot.hash, snapshotInfo)
      _ <- snapshotService.setSnapshot(snapshotInfo).attemptT
    } yield ()

  private def acceptSnapshot(storedSnapshot: StoredSnapshot, height: Long): EitherT[F, Throwable, Unit] =
    for {
      _ <- snapshotLocalStorage.write(storedSnapshot.snapshot.hash, storedSnapshot)

      ownJoinedHeight = height - snapshotHeightInterval

      _ <- cluster.setOwnJoinedHeight(ownJoinedHeight).attemptT

      _ <- redownloadService
        .persistAcceptedSnapshot(height, storedSnapshot.snapshot.hash)
        .attemptT

      _ <- redownloadService
        .persistCreatedSnapshot(height, storedSnapshot.snapshot.hash, storedSnapshot.snapshot.publicReputation)
        .attemptT

      _ <- redownloadService.setLastMajorityState(Map(height -> storedSnapshot.snapshot.hash)).attemptT
      _ <- redownloadService.setLastSentHeight(height).attemptT
    } yield ()

  private def acceptRewards(storedRewards: StoredRewards, height: Long): EitherT[F, Throwable, Unit] =
    for {
      _ <- eigenTrust.setModel(storedRewards.model).attemptT
      _ <- rewardsManager.setLastRewardedHeight(height).attemptT
    } yield ()

  private def validateAccountBalance(accountBalances: AccountBalances): EitherT[F, Throwable, Unit] =
    EitherT.fromEither {
      if (accountBalances.exists { case (_, balance) => balance < 0 }) Left(InvalidBalances) else Right(())
    }
}
