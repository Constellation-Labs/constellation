package org.constellation.datastore.swaydb

import swaydb.data.config.MMAP

import scala.concurrent.ExecutionContextExecutor
import constellation._
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.datastore.{KVDB, KVDBDatastoreImpl}
import org.constellation.serializer.KryoSerializer

class SwayDBImpl(dao: DAO) extends KVDB {

  implicit val d: DAO = dao

  import swaydb._
  import swaydb.serializers.Default._ //import default serializers

  private implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.bounded

  //Create a persistent database. If the directories do not exist, they will be created.
  private val db =
    persistent
      .Map[String, Array[Byte]](
        dir = (dao.dbPath / "disk1").path,
        mmapMaps = false,
        mmapSegments = MMAP.Disabled,
        mmapAppendix = false
      )
      .get

  override def put(key: String, obj: AnyRef): Boolean = {
    val triedMeter = db.put(key, KryoSerializer.serializeAnyRef(obj))
    tryToMetric(triedMeter.toTry, "dbPutAttempt")

    /*    val getCheckAttempt = get[AnyRef](key)
    if (getCheckAttempt.isEmpty) {
      dao.metricsManager ! IncrementMetric("dbPutVerificationFailed")
    }*/

    triedMeter.isSuccess
  }

  override def get[T <: AnyRef](key: String): Option[T] = {
    val triedMaybeBytes = db.get(key)
    tryToMetric(triedMaybeBytes.toTry, "dbGetAttempt")

    triedMaybeBytes.toOption.flatten.flatMap { ab =>
      tryWithMetric({ KryoSerializer.deserialize(ab).asInstanceOf[T] }, "kryoDeserializeDB").toOption
    }
  }

  override def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T = {
    val res = get(key).map { updateF }
    if (res.isEmpty) {
      put(key, empty)
      empty
    } else {
      val res2 = res.get
      put(key, res2)
      res2
    }
  }

  override def delete(key: String): Boolean =
    db.remove(key).isSuccess

  override def restart(): Unit = {}
}

object SwayDBImpl {

  def apply(dao: DAO): SwayDBImpl = new SwayDBImpl(dao)
}

class SwayDBDatastore(dao: DAO) extends KVDBDatastoreImpl {
  //val kvdb = KVDBAuditProxy(SwayDBImpl(dao))
  val kvdb = SwayDBImpl(dao)
}

object SwayDBDatastore {

  def duplicateCheckStore(dao: DAO, path: String): swaydb.Set[String] = {

    import swaydb._
    import swaydb.serializers.Default._ //import default serializers

    persistent
      .Set[String](
        dir = (dao.dbPath / path).path,
        mmapMaps = false,
        mmapSegments = MMAP.Disabled,
        mmapAppendix = false
      )
      .get
  }

  def apply(dao: DAO): SwayDBDatastore = new SwayDBDatastore(dao)
}
