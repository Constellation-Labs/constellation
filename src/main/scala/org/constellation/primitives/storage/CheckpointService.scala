package org.constellation.primitives.storage

import better.files.File
import cats.effect.IO
import cats.implicits._
import org.constellation.DAO
import org.constellation.datastore.swaydb.SwayDbConversions._
import org.constellation.primitives.Schema.CheckpointCacheData
import swaydb.serializers.Default.StringSerializer

import scala.concurrent.ExecutionContextExecutor

object CheckpointBlocksOld {
  def apply(dao: DAO) = new CheckpointBlocksOld(dao.dbPath)(dao.edgeExecutionContext)
}

class CheckpointBlocksOld(path: File)(implicit ec: ExecutionContextExecutor)
  extends DbStorage[String, CheckpointCacheData](dbPath = (path / "disk1" / "checkpoints_old").path) {}

object CheckpointBlocksMid {
  val midCapacity = 1

  def apply(dao: DAO) = new CheckpointBlocksMid(dao.dbPath, midCapacity)(dao.edgeExecutionContext)
}

class CheckpointBlocksMid(path: File, midCapacity: Int)(implicit ec: ExecutionContextExecutor)
  extends MidDbStorage[String, CheckpointCacheData](dbPath = (path / "disk1" / "checkpoints_mid").path, midCapacity) {}

// TODO: Make separate one for acceptedCheckpoints vs nonresolved etc.
// mwadon: /\ is still relevant?
class CheckpointBlocksMemPool(size: Int = 50000) extends StorageService[CheckpointCacheData](size)

object CheckpointService {
  def apply(implicit dao: DAO, size: Int = 50000) = new CheckpointService(dao, size)
}

class CheckpointService(dao: DAO, size: Int = 50000) {
  val memPool = new CheckpointBlocksMemPool(size)
  val midDb: MidDbStorage[String, CheckpointCacheData] = CheckpointBlocksMid(dao)
  val oldDb: DbStorage[String, CheckpointCacheData] = CheckpointBlocksOld(dao)

  def migrateOverCapacity(): IO[Unit] = {
    midDb.pullOverCapacity()
      .flatMap(_.map(cb => oldDb.put(cb.checkpointBlock.get.baseHash, cb)).sequence[IO, Unit])
      .map(_ => ())
  }

  def lookup: String => IO[Option[CheckpointCacheData]] =
    DbStorage.extendedLookup[String, CheckpointCacheData](List(memPool, midDb, oldDb))

  def get(key: String) = lookup(key).unsafeRunSync()
  def contains(key: String) = lookup(key).map(_.nonEmpty).unsafeRunSync()
}

