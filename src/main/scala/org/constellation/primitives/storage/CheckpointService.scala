package org.constellation.primitives.storage

import better.files.File
import cats.effect.IO
import cats.implicits._
import org.constellation.DAO
import org.constellation.datastore.swaydb.SwayDbConversions._
import org.constellation.primitives.Schema.{CheckpointCacheData, CheckpointCacheFullData}
import org.constellation.primitives._
import swaydb.serializers.Default.StringSerializer

import scala.concurrent.ExecutionContextExecutor

object CheckpointBlocksOld {
  def apply(dao: DAO) = new CheckpointBlocksOld(dao.dbPath)(dao.edgeExecutionContext)
}

class CheckpointBlocksOld(path: File)(implicit ec: ExecutionContextExecutor)
    extends DbStorage[String, CheckpointCacheData](
      dbPath = (path / "disk1" / "checkpoints_old").path
    )

object CheckpointBlocksMid {
  val midCapacity = 1

  def apply(dao: DAO) = new CheckpointBlocksMid(dao.dbPath, midCapacity)(dao.edgeExecutionContext)
}

class CheckpointBlocksMid(path: File, midCapacity: Int)(implicit ec: ExecutionContextExecutor)
    extends MidDbStorage[String, CheckpointCacheData](dbPath =
                                                        (path / "disk1" / "checkpoints_mid").path,
                                                      midCapacity)

// TODO: Make separate one for acceptedCheckpoints vs nonresolved etc.
// mwadon: /\ is still relevant?
class CheckpointBlocksMemPool(size: Int = 50000)(implicit dao: DAO)
    extends StorageService[CheckpointCacheData](size) {
  override def putSync(
    key: String,
    value: CheckpointCacheData
  ): CheckpointCacheData = {
    incrementChildrenCount(value.cb.parentSOEBaseHashes())
    super.putSync(key, value)
  }

  def incrementChildrenCount(hashes: Seq[String]): Unit = {
    hashes.foreach(
      hash =>
        update(hash, (cd: CheckpointCacheData) => { cd.copy(children = cd.children + 1) })
          .unsafeRunAsyncAndForget()
    )
  }

}

object CheckpointService {
  def apply(implicit dao: DAO, size: Int = 50000) = new CheckpointService(dao, size)

  def fetchFullData(cacheData: CheckpointCacheData)(implicit dao: DAO): CheckpointCacheFullData = {
    CheckpointCacheFullData(
      Some(
        CheckpointBlockFullData(
          fetchTransactions(cacheData.cb.transactionsMerkleRoot),
          cacheData.cb.checkpoint,
          fetchMessages(cacheData.cb.messagesMerkleRoot),
          fetchNotifications(cacheData.cb.notificationsMerkleRoot)
        )
      ),
      cacheData.children,
      Some(cacheData.height)
    )
  }
  def fetchTransactions(transactionsMerkleRoot: String)(implicit dao: DAO): Seq[Transaction] = {

    dao.transactionService.merklePool
      .get(transactionsMerkleRoot)
      .map(opt => opt.getOrElse(Seq.empty))
      .map(
        s =>
          s.map(
              dao.transactionService
                .lookup(_)
                .map(maybeT => maybeT.get.transaction)
            )
            .toList
            .sequence[IO, Transaction]
      )
      .flatten
      .unsafeRunSync()
  }

  def fetchMessages(messagesMerkleRoot: String)(implicit dao: DAO): Seq[ChannelMessage] = {
    dao.messageService.merklePool
      .get(messagesMerkleRoot)
      .map(opt => opt.getOrElse(Seq.empty))
      .map(
        s =>
          s.map(
              dao.messageService
                .lookup(_)
                .map(maybeM => maybeM.get.channelMessage)
            )
            .toList
            .sequence[IO, ChannelMessage]
      )
      .flatten
      .unsafeRunSync()
  }

  def fetchNotifications(notificationsMerkleRoot: String)(
    implicit dao: DAO
  ): Seq[PeerNotification] = {
    dao.notificationService.merklePool
      .get(notificationsMerkleRoot)
      .map(opt => opt.getOrElse(Seq.empty))
      .map(
        s =>
          s.map(
              dao.notificationService
                .lookup(_)
                .map(maybeM => maybeM.get)
            )
            .toList
            .sequence[IO, PeerNotification]
      )
      .flatten
      .unsafeRunSync()
  }
}

class CheckpointService(dao: DAO, size: Int = 50000) {
  val memPool = new CheckpointBlocksMemPool(size)(dao)
  val midDb: MidDbStorage[String, CheckpointCacheData] = CheckpointBlocksMid(dao)
  val oldDb: DbStorage[String, CheckpointCacheData] = CheckpointBlocksOld(dao)

  def migrateOverCapacity(): IO[Unit] = {
    midDb
      .pullOverCapacity()
      .flatMap(_.map(cd => oldDb.put(cd.cb.baseHash, cd)).sequence[IO, Unit])
      .map(_ => ())
  }

  def lookup: String => IO[Option[CheckpointCacheData]] =
    DbStorage.extendedLookup[String, CheckpointCacheData](List(memPool, midDb, oldDb))

  def get(key: String) = lookup(key).unsafeRunSync()
  def contains(key: String) = lookup(key).map(_.nonEmpty).unsafeRunSync()
}
