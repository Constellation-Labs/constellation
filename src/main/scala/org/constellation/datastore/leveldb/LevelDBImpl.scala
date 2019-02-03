package org.constellation.datastore.leveldb
import better.files._
import com.typesafe.scalalogging.Logger
import org.constellation.DAO
import org.constellation.datastore.KVDB
import org.constellation.serializer.KryoSerializer

import scala.util.Try

/** Documentation. */
class LevelDBImpl(dao: DAO) extends KVDB {
  private val logger = Logger("KVDB")

  /** Documentation. */
  private def tmpDirId = file"tmp/${dao.id.medium}/db"

  /** Documentation. */
  private def mkDB: LevelDB = LevelDB(tmpDirId)

  private var db = mkDB

  /** Documentation. */
  override def put(key: String, obj: AnyRef): Boolean = {
    dao.metrics.incrementMetric("DBPut")
    val bytes = KryoSerializer.serializeAnyRef(obj)
    val success = db.putBytes(key, bytes)
    if (!success) {
      dao.metrics.incrementMetric("DBPutFailure")
      logger.error("DB PUT FAILED")
    }
    success
  }

  /** Documentation. */
  override def get[T <: AnyRef](key: String): Option[T] = {
    db.getBytes(key)
      .map(bytes => KryoSerializer.deserialize(bytes).asInstanceOf[T])
  }

  /** Documentation. */
  override def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T = {
    val o = get[T](key)
      .map(updateF)
      .getOrElse(empty)
    put(key, o)
    o
  }

  /** Documentation. */
  override def restart(): Unit = {
    Try { db.destroy() }
      .foreach(e => logger.warn("Exception while destroying LevelDB db", e))
    db = mkDB
  }

  /** Documentation. */
  override def delete(key: String): Boolean =
    if (db.contains(key)) {
      db.delete(key).isSuccess
    } else true

  /** Documentation. */
  private def getUnsafe[T <: AnyRef](key: String): Option[AnyRef] = {
    db.getBytes(key).map(bytes => KryoSerializer.deserialize(bytes))
  }
}

