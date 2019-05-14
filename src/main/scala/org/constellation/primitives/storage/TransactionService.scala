package org.constellation.primitives.storage

import better.files.File
import cats.effect.IO
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation.DAO
import org.constellation.datastore.swaydb.SwayDbConversions._
import org.constellation.primitives.TransactionCacheData
import swaydb.serializers.Default.StringSerializer

import scala.concurrent.ExecutionContextExecutor

object TransactionsOld {
  def apply(dao: DAO) = new TransactionsOld(dao.dbPath)(dao.edgeExecutionContext)
}

class TransactionsOld(path: File)(implicit ec: ExecutionContextExecutor)
  extends DbStorage[String, TransactionCacheData](dbPath = (path / "disk1" / "transactions_old").path)

object TransactionsMid {
  val midCapacity = 1

  def apply(dao: DAO) = new TransactionsMid(dao.dbPath, midCapacity)(dao.edgeExecutionContext)
}

class TransactionsMid(path: File, midCapacity: Int)(implicit ec: ExecutionContextExecutor)
  extends MidDbStorage[String, TransactionCacheData](dbPath = (path / "disk1" / "transactions_mid").path, midCapacity)


class TransactionMemPool(size: Int = 50000) extends StorageService[TransactionCacheData](size, Some(45))

object TransactionService {
  def apply(implicit dao: DAO, size: Int = 50000) = new TransactionService(dao, size)
}

class TransactionMidPool(size: Int = 50000) extends TransactionMemPool(size)
class TransactionOldPool(size: Int = 50000) extends TransactionMemPool(size)

class TransactionService(dao: DAO, size: Int = 50000) extends MerkleService[TransactionCacheData] with StrictLogging {
  val merklePool = new StorageService[Seq[String]](size)
  val arbitraryPool = new TransactionMemPool(size)
  val memPool = new TransactionMemPool(size)
  val midDb = new TransactionMidPool(size)
  val oldDb = new TransactionOldPool(size)
//  val midDb: MidDbStorage[String, TransactionCacheData] = TransactionsMid(dao)
//  val oldDb: DbStorage[String, TransactionCacheData] = TransactionsOld(dao)

//  def migrateOverCapacity(): IO[Unit] = {
//    midDb.pullOverCapacity()
//      .flatMap(_.map(tx => oldDb.put(tx.transaction.hash, tx)).sequence[IO, Unit])
//      .map(_ => ())
//  }

  override def lookup: String => IO[Option[TransactionCacheData]] =
    DbStorage.extendedLookup[String, TransactionCacheData](List(memPool, midDb, oldDb))


  def contains: String ⇒ IO[Boolean] =
    DbStorage.extendedContains[String, TransactionCacheData](List(memPool, midDb, oldDb))

  override def findHashesByMerkleRoot(merkleRoot: String): IO[Option[Seq[String]]] =
    merklePool.get(merkleRoot)
}

