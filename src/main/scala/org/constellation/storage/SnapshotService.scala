package org.constellation.storage

import better.files.File
import cats.effect.IO
import cats.implicits._
import org.constellation.DAO
import org.constellation.consensus.Snapshot
import org.constellation.datastore.swaydb.SwayDbConversions._
import swaydb.serializers.Default._

import scala.concurrent.ExecutionContextExecutor

object SnapshotsOldDbStorage {
  def apply(dao: DAO) = new SnapshotsOldDbStorage(dao.dbPath)(dao.edgeExecutionContext)
}

class SnapshotsOldDbStorage(path: File)(implicit ec: ExecutionContextExecutor)
  extends DbStorage[String, Snapshot](dbPath = (path / "disk1" / "snapshots_old").path)

object SnapshotsMidDbStorage {
  val midCapacity = 1

  def apply(dao: DAO) = new SnapshotsMidDbStorage(dao.dbPath, midCapacity)(dao.edgeExecutionContext)
}

class SnapshotsMidDbStorage(path: File, midCapacity: Int)(implicit ec: ExecutionContextExecutor)
    extends MidDbStorage[String, Snapshot](dbPath = (path / "disk1" / "snapshots_mid").path, 1)

object SnapshotService {
  def apply(implicit dao: DAO) = new SnapshotService()
}

class SnapshotService(implicit dao: DAO) {
  val midDb: MidDbStorage[String, Snapshot] = SnapshotsMidDbStorage(dao)
  val oldDb: DbStorage[String, Snapshot] = SnapshotsOldDbStorage(dao)

  def migrateOverCapacity(): IO[Unit] = {
    midDb.pullOverCapacity()
      .flatMap(_.map(snapshot => oldDb.put(snapshot.hash, snapshot)).sequence[IO, Unit])
      .map(_ => ())
  }

  def lookup: String => IO[Option[Snapshot]] =
    DbStorage.extendedLookup[String, Snapshot](List(midDb, oldDb))
}