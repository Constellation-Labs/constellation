package org.constellation.datastore.leveldb

import better.files._
import com.typesafe.scalalogging.Logger

import org.constellation.DAO
import org.constellation.datastore.KVDB
import org.constellation.primitives.IncrementMetric
import org.constellation.serializer.KryoSerializer

import scala.util.Try

/** Level database implementation class.
  *
  * @param dao ... Data access object.
  */
class LevelDBImpl(dao: DAO) extends KVDB {
  private val logger = Logger("KVDB")

  /** @return The database directory. */
  private def tmpDirId = file"tmp/${dao.id.medium}/db"

  /** @return A databse for the specified directory. */
  private def mkDB: LevelDB = LevelDB(tmpDirId)

  private var db = mkDB

  /** Put method. */
  override def put(key: String, obj: AnyRef): Boolean = {
    dao.metricsManager ! IncrementMetric("DBPut")
    val bytes = KryoSerializer.serializeAnyRef(obj)
    val success = db.putBytes(key, bytes)
    if (!success) {
      dao.metricsManager ! IncrementMetric("DBPutFailure")
      logger.error("DB PUT FAILED")
    }
    success
  }

  /** Getter for the entry corresponding to input key. */
  override def get[T <: AnyRef](key: String): Option[T] = {
    db.getBytes(key)
      .map(bytes => KryoSerializer.deserialize(bytes).asInstanceOf[T])
  }

  /** Update the entry corresponding to input key. */
  override def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T = {
    val o = get[T](key)
      .map(updateF)
      .getOrElse(empty)
    put(key, o)
    o
  }

  /** Reset. */
  override def restart(): Unit = {
    Try {
      db.destroy()
    }
      .foreach(e => logger.warn("Exception while destroying LevelDB db", e))
    db = mkDB
  }

  /** Delete the entry corresponding to input key. */
  override def delete(key: String): Boolean =
    if (db.contains(key)) {
      db.delete(key).isSuccess
    } else true

  // doc
  private def getUnsafe[T <: AnyRef](key: String): Option[AnyRef] = {
    db.getBytes(key).map(bytes => KryoSerializer.deserialize(bytes))
  }

} // end LevelDBImpl
