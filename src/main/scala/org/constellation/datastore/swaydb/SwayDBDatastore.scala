package org.constellation.datastore.swaydb

import constellation._
import org.constellation.DAO
import org.constellation.datastore.proxy.KVDBAuditProxy
import org.constellation.datastore.{KVDB, KVDBDatastoreImpl}
import org.constellation.primitives.IncrementMetric
import org.constellation.serializer.KryoSerializer
import swaydb.data.config.MMAP

class SwayDBImpl(dao: DAO) extends KVDB {

  implicit val d: DAO = dao

  import swaydb._
  import swaydb.serializers.Default._ //import default serializers

  //Create a persistent database. If the directories do not exist, they will be created.
  private val db = SwayDB.persistent[String, Array[Byte]](
    dir = (dao.dbPath / "disk1").path,
    mmapMaps = false,
    mmapSegments = MMAP.Disable,
    mmapAppendix = false
  ).get

  override def put(key: String, obj: AnyRef): Boolean = {
    val triedMeter = db.put(key, KryoSerializer.serializeAnyRef(obj))
    tryToMetric(triedMeter, "dbPutAttempt")

/*    val getCheckAttempt = get[AnyRef](key)
    if (getCheckAttempt.isEmpty) {
      dao.metricsManager ! IncrementMetric("dbPutVerificationFailed")
    }*/

    triedMeter.isSuccess
  }

  override def get[T <: AnyRef](key: String): Option[T] = {
    val triedMaybeBytes = db.get(key)
    tryToMetric(triedMaybeBytes, "dbGetAttempt")
    triedMaybeBytes.toOption.flatMap { a =>
      a.flatMap { ab => tryWithMetric({KryoSerializer.deserialize(ab).asInstanceOf[T]}, "kryoDeserializeDB").toOption }

    }
  }

  override def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T = {
    val res = get(key).map{updateF}
    if (res.isEmpty) {
      put(key, empty)
      empty
    } else {
      val res2 = res.get
      put(key, res2)
      res2
    }
  }

  override def delete(key: String): Boolean = {
    db.remove(key).isSuccess
  }

  override def restart(): Unit = {

  }
}

object SwayDBImpl {
  def apply(dao: DAO): SwayDBImpl = new SwayDBImpl(dao)
}

class SwayDBDatastore(dao: DAO) extends KVDBDatastoreImpl {
  //val kvdb = KVDBAuditProxy(SwayDBImpl(dao))
  val kvdb = SwayDBImpl(dao)
}

object SwayDBDatastore {
  def apply(dao: DAO): SwayDBDatastore = new SwayDBDatastore(dao)
}

