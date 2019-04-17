package org.constellation.primitives.storage

import better.files.File
import cats.effect.IO
import cats.implicits._
import org.constellation.DAO
import org.constellation.datastore.swaydb.SwayDbConversions._
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{CheckpointCacheData, CheckpointCacheDataMerkle}
import org.constellation.primitives._
import org.constellation.util.MerkleTree
import swaydb.serializers.Default.StringSerializer

import scala.concurrent.ExecutionContextExecutor

object CheckpointBlocksOld {
  def apply(dao: DAO) = new CheckpointBlocksOld(dao.dbPath)(dao.edgeExecutionContext)
}

class CheckpointBlocksOld(path: File)(implicit ec: ExecutionContextExecutor)
    extends DbStorage[String, CheckpointCacheDataMerkle](
      dbPath = (path / "disk1" / "checkpoints_old").path
    )

object CheckpointBlocksMid {
  val midCapacity = 1

  def apply(dao: DAO) = new CheckpointBlocksMid(dao.dbPath, midCapacity)(dao.edgeExecutionContext)
}

class CheckpointBlocksMid(path: File, midCapacity: Int)(implicit ec: ExecutionContextExecutor)
    extends MidDbStorage[String, CheckpointCacheDataMerkle](
        dbPath = (path / "disk1" / "checkpoints_mid").path,
        midCapacity
      )

// TODO: Make separate one for acceptedCheckpoints vs nonresolved etc.
// mwadon: /\ is still relevant?
class CheckpointBlocksMemPool(size: Int = 50000)(implicit dao: DAO)
    extends StorageService[CheckpointCacheDataMerkle](size) {

  def putSync(
    key: String,
    value: CheckpointCacheData
  ): CheckpointCacheDataMerkle = {
    value.checkpointBlock.foreach(cb => incrementChildrenCount(cb.parentSOEBaseHashes()))

    super.putSync(key, CheckpointCacheDataMerkle(storeMerkleRoots(value.checkpointBlock.get),
                                            value.children,
                                            value.height))
  }

  def storeMerkleRoots(data: CheckpointBlock): CheckpointBlockData = {
    val t = store(data.transactions.map(_.hash), dao.transactionService.merklePool).get
    val m = store(data.messages.map(_.signedMessageData.hash), dao.messageService.merklePool)
    val n = store(data.notifications.map(_.hash), dao.notificationService.merklePool)

    CheckpointBlockData(
      t,
      data.checkpoint,
      m,
      n,
    )
  }

  private def store(data: Seq[String], ss: StorageService[Seq[String]]): Option[String] = {
    data match {
      case Seq() => None
      case _ =>
        val rootHash = MerkleTree(data).rootHash
        ss.putSync(rootHash, data)
        Some(rootHash)
    }
  }


  def incrementChildrenCount(hashes: Seq[String]): Unit = {
    hashes.foreach(
      hash =>
        update(hash, (cd: CheckpointCacheDataMerkle) => { cd.copy(children = cd.children + 1) })
          .unsafeRunAsyncAndForget()
    )
  }

}

object CheckpointService {
  def apply(implicit dao: DAO, size: Int = 50000) = new CheckpointService(dao, size)

  def convert(merkle: CheckpointCacheDataMerkle)(implicit dao: DAO): CheckpointCacheData = {
    val txs = fetchTransactions(merkle.checkpointBlock.transactionsMerkleRoot)
    val msgs =
      merkle.checkpointBlock.messagesMerkleRoot
        .fold(Seq[ChannelMessage]())(mr => fetchMessages(mr))
    val notifications =
      merkle.checkpointBlock.notificationsMerkleRoot.fold(Seq[PeerNotification]())(mr => fetchNotifications(mr))
    CheckpointCacheData(
      Some(
        CheckpointBlock(
          txs,
          merkle.checkpointBlock.checkpoint,
          msgs,
          notifications
        )
      ),
      merkle.children,
      merkle.height
    )
  }

  def fetchTransactions(
    merkleRoot: String
  )(implicit dao: DAO): Seq[Transaction] = {

    fetch[TransactionCacheData,Transaction](
      merkleRoot,
      dao.transactionService,
      (x: TransactionCacheData) => x.transaction,
      (s: String) =>
        DataResolver.resolveTransactionsDefaults(s).map(_.get)
    )
  }

  def fetchMessages(merkleRoot: String)(implicit dao: DAO): Seq[ChannelMessage] = {
    fetch[ChannelMessageMetadata,ChannelMessage](
      merkleRoot,
      dao.messageService,
      (x: ChannelMessageMetadata) => x.channelMessage,
      (s: String) =>
        DataResolver.resolveMessagesDefaults(s).map(_.get)
    )
  }

  def fetch[T, R](
    merkleRoot: String,
    service: MerkleService[T],
    mapper: T => R,
    resolver: String => IO[T],
  )(implicit dao: DAO): Seq[R] = {
    service.findHashesByMerkleRoot(merkleRoot)
      .map(
        _.get
          .map(hash => service.lookup(hash)
            .flatMap(t => t.map(IO.pure).getOrElse(resolver(hash)).map(mapper)))
      )
      .map(_.toList.sequence)
      .flatten
      .unsafeRunSync()
  }

  def fetchNotifications(merkleRoot: String)(
    implicit dao: DAO
  ): Seq[PeerNotification] = {
    fetch[PeerNotification,PeerNotification](
      merkleRoot,
      dao.notificationService,
      (x: PeerNotification) => x,
      (s: String) => ???
    )
  }
}

class CheckpointService(dao: DAO, size: Int = 50000) {
  val memPool = new CheckpointBlocksMemPool(size)(dao)
  val midDb: MidDbStorage[String, CheckpointCacheDataMerkle] = CheckpointBlocksMid(dao)
  val oldDb: DbStorage[String, CheckpointCacheDataMerkle] = CheckpointBlocksOld(dao)

  def migrateOverCapacity(): IO[Unit] = {
    midDb
      .pullOverCapacity()
      .flatMap(_.map(cb => oldDb.put(cb.checkpointBlock.baseHash, cb)).sequence[IO, Unit])
      .map(_ => ())
  }

  def lookup: String => IO[Option[CheckpointCacheDataMerkle]] =
    DbStorage.extendedLookup[String, CheckpointCacheDataMerkle](List(memPool, midDb, oldDb))

  def lookupFullData: String => IO[Option[CheckpointCacheData]] =
    DbStorage
      .extendedLookup[String, CheckpointCacheDataMerkle](List(memPool, midDb, oldDb))
      .map(_.map(_.map(CheckpointService.convert(_)(dao))))

  def get(key: String) = lookup(key).unsafeRunSync()
  def getFullData(key: String) = lookup(key).map(_.map(CheckpointService.convert(_)(dao))).unsafeRunSync()
  def contains(key: String) = lookup(key).map(_.nonEmpty).unsafeRunSync()


}
