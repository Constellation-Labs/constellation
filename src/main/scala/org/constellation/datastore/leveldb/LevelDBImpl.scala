package org.constellation.datastore.leveldb

import better.files._
import com.typesafe.scalalogging.Logger
import scala.util.Try

import org.constellation.DAO
import org.constellation.datastore.KVDB
import org.constellation.serializer.KryoSerializer

class LevelDBImpl(dao: DAO) extends KVDB {
  private val logger = Logger("KVDB")

  private def tmpDirId = file"tmp/${dao.id.medium}/db"

  private def mkDB: LevelDB = LevelDB(tmpDirId)

  private var db = mkDB

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

  override def get[T <: AnyRef](key: String): Option[T] = {
    db.getBytes(key)
      .map(bytes => KryoSerializer.deserialize(bytes).asInstanceOf[T])
  }

  override def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T = {
    val o = get[T](key)
      .map(updateF)
      .getOrElse(empty)
    put(key, o)
    o
  }

  override def restart(): Unit = {
    Try { db.destroy() }
      .foreach(e => logger.warn("Exception while destroying LevelDB db", e))
    db = mkDB
  }

  override def delete(key: String): Boolean =
    if (db.contains(key)) {
      db.delete(key).isSuccess
    } else true

  private def getUnsafe[T <: AnyRef](key: String): Option[AnyRef] = {
    db.getBytes(key).map(bytes => KryoSerializer.deserialize(bytes))
  }
}
