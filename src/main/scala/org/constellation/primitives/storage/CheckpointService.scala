package org.constellation.primitives.storage

import better.files.File
import cats.effect.IO
import cats.implicits._
import org.constellation.DAO
import org.constellation.datastore.swaydb.SwayDbConversions._
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{CheckpointCacheData, CheckpointCacheFullData}
import org.constellation.primitives._
import org.constellation.util.PeerApiClient
import swaydb.serializers.Default.StringSerializer

import scala.concurrent.ExecutionContextExecutor
import scala.reflect.ClassTag

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
    val txs = fetchTransactions(cacheData.cb.transactionsMerkleRoot)
    val msgs = cacheData.cb.messagesMerkleRoot.fold(Seq[ChannelMessage]())(mr => fetchMessages(mr))
    val notifications =
      cacheData.cb.notificationsMerkleRoot.fold(Seq[PeerNotification]())(mr => fetchNotifications(mr))
    CheckpointCacheFullData(
      Some(
        CheckpointBlockFullData(
          txs,
          cacheData.cb.checkpoint,
          msgs,
          notifications
        )
      ),
      cacheData.children,
      cacheData.height
    )
  }

//  def orElseThrow[T](merkleRoot: String, data: Option[Seq[T]])(implicit m: ClassTag[T]): Seq[T] = {
//    data match {
//      case None    => throw MerkleRootMappingException(merkleRoot, m.runtimeClass.getSimpleName)
//      case Some(x) => x
//    }
//  }

  def fetchTransactions(
    merkleRoot: String
  )(implicit dao: DAO): Seq[Transaction] = {

    fetch[TransactionCacheData,Transaction](
      merkleRoot,
      dao.transactionService,
      (x: TransactionCacheData) => x.transaction,
      (s: String) =>
        DataResolver.resolveTransactions(s,
                                         dao.readyPeers.map(p => PeerApiClient(p._1, p._2.client))).map(_.get)
    )
  }

  def fetchMessages(merkleRoot: String)(implicit dao: DAO): Seq[ChannelMessage] = {
    fetch[ChannelMessageMetadata,ChannelMessage](
      merkleRoot,
      dao.messageService,
      (x: ChannelMessageMetadata) => x.channelMessage,
      (s: String) =>
        DataResolver.resolveMessages(s,
          dao.readyPeers.map(p => PeerApiClient(p._1, p._2.client))).map(_.get)
    )
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

  def fetch[T, R](
    merkleRoot: String,
    service: MerkleService[T],
    mapper: T => R,
    resolver: String => IO[T],
  )(implicit dao: DAO): Seq[R] = {
    service.merklePool
      .get(merkleRoot)
      .map(
        _.getOrElse(Seq.empty)
          .map(hash => service.lookup(hash).map(t => mapper(t.getOrElse(resolver(hash).unsafeRunSync()))))
      )
      .map(_.toList.sequence)
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
case class MerkleRootMappingException(root: String, t: String)
      extends Exception(s"Unable to obtain data for root: $root and type:")