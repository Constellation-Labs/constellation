package org.constellation

import akka.actor.{Actor, ActorSystem}
import akka.util.Timeout
import better.files._
import com.typesafe.scalalogging.Logger
import constellation.{ParseExt, SerExt}
import org.constellation.LevelDB.RestartDB
import org.constellation.primitives.Schema._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.ProductHash
import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}

// https://doc.akka.io/docs/akka/2.5/persistence-query-leveldb.html

object LevelDB {

  case object RestartDB
  case class DBGet(key: String)
  case class DBPut(key: String, obj: AnyRef)
  case class DBDelete(key: String)
  case class DBUpdate[T <: AnyRef](key: String, f: T => T, empty: T)

  def apply(file: File) = {
    new LevelDB(file)
  }

}

trait KVDB {

  def restart(): Unit
  def delete(key: String): Boolean
  def getSnapshot(key: String) : Option[consensus.Snapshot]
  def putSnapshot(key: String, snapshot: consensus.Snapshot): Unit
  def putCheckpointCacheData(key: String, c: CheckpointCacheData): Unit
  def updateCheckpointCacheData(key: String,
                                f: CheckpointCacheData => CheckpointCacheData,
                                empty: CheckpointCacheData): CheckpointCacheData
  def getCheckpointCacheData(key: String): Option[CheckpointCacheData]
  def putTransactionCacheData(key: String, t: TransactionCacheData): Unit
  def updateTransactionCacheData(
    key: String,
    f: TransactionCacheData => TransactionCacheData,
    empty: TransactionCacheData
  ): TransactionCacheData
  def getTransactionCacheData(key: String): Option[TransactionCacheData]
  def putAddressCacheData(key: String, t: AddressCacheData): Unit
  def updateAddressCacheData(key: String,
                             f: AddressCacheData => AddressCacheData,
                             empty: AddressCacheData): AddressCacheData
  def getAddressCacheData(key: String): Option[AddressCacheData]
  def putSignedObservationEdgeCache(key: String,
                                    t: SignedObservationEdgeCache): Unit
  def updateSignedObservationEdgeCache(
    key: String,
    f: SignedObservationEdgeCache => SignedObservationEdgeCache,
    empty: SignedObservationEdgeCache
  ): SignedObservationEdgeCache
  def getSignedObservationEdgeCache(
    key: String
  ): Option[SignedObservationEdgeCache]
  def putTransactionEdgeData(key: String, t: TransactionEdgeData): Unit
  def updateTransactionEdgeData(key: String,
                                f: TransactionEdgeData => TransactionEdgeData,
                                empty: TransactionEdgeData): TransactionEdgeData
  def getTransactionEdgeData(key: String): Option[TransactionEdgeData]
  def putCheckpointEdgeData(key: String, t: CheckpointEdgeData): Unit
  def updateCheckpointEdgeData(key: String,
                               f: CheckpointEdgeData => CheckpointEdgeData,
                               empty: CheckpointEdgeData): CheckpointEdgeData
  def getCheckpointEdgeData(key: String): Option[CheckpointEdgeData]
}

class KVDBImpl(dao: DAO) extends KVDB {
  private val logger = Logger("KVDB")

  private def tmpDirId = file"tmp/${dao.id.medium}/db"
  private def mkDB: LevelDB = LevelDB(tmpDirId)

  private var db = mkDB

  private def put(key: String, obj: AnyRef) = {
    dao.numDBPuts += 1
    val bytes = KryoSerializer.serializeAnyRef(obj)
    db.putBytes(key, bytes)
  }

  private def get[T <: AnyRef](key: String): Option[T] = {
    dao.numDBGets += 1
    db.getBytes(key)
      .map(bytes => KryoSerializer.deserialize(bytes).asInstanceOf[T])
  }

  private def getUnsafe[T <: AnyRef](key: String): Option[AnyRef] = {
    dao.numDBGets += 1
    db.getBytes(key).map(bytes => KryoSerializer.deserialize(bytes))
  }

  private def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T = {
    dao.numDBUpdates += 1
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
      dao.numDBDeletes += 1
      db.delete(key).isSuccess
    } else true

  override def putSnapshot(key: String, snapshot: consensus.Snapshot): Unit = put(key, snapshot)

  override def getSnapshot(key: String): Option[consensus.Snapshot] = get[consensus.Snapshot](key)

  override def putCheckpointCacheData(s: String, c: CheckpointCacheData): Unit =
    put(s, c)

  override def getCheckpointCacheData(s: String): Option[CheckpointCacheData] =
    get[CheckpointCacheData](s)

  override def putTransactionCacheData(s: String,
                                       t: TransactionCacheData): Unit =
    put(s, t)
  override def getTransactionCacheData(
    s: String
  ): Option[TransactionCacheData] = get[TransactionCacheData](s)

  override def updateCheckpointCacheData(
    key: String,
    f: CheckpointCacheData => CheckpointCacheData,
    empty: CheckpointCacheData
  ): CheckpointCacheData = update(key, f, empty)

  override def updateTransactionCacheData(
    key: String,
    f: TransactionCacheData => TransactionCacheData,
    empty: TransactionCacheData
  ): TransactionCacheData = update(key, f, empty)

  override def putAddressCacheData(key: String, t: AddressCacheData): Unit =
    put(key, t)

  override def updateAddressCacheData(
    key: String,
    f: AddressCacheData => AddressCacheData,
    empty: AddressCacheData
  ): AddressCacheData = update(key, f, empty)

  override def getAddressCacheData(key: String): Option[AddressCacheData] = {
    get[AddressCacheData](key)
  }

  override def putSignedObservationEdgeCache(
    key: String,
    t: SignedObservationEdgeCache
  ): Unit =
    put(key, t)

  override def updateSignedObservationEdgeCache(
    key: String,
    f: SignedObservationEdgeCache => SignedObservationEdgeCache,
    empty: SignedObservationEdgeCache
  ): SignedObservationEdgeCache =
    update(key, f, empty)

  override def getSignedObservationEdgeCache(
    key: String
  ): Option[SignedObservationEdgeCache] =
    get[SignedObservationEdgeCache](key)
  override def putTransactionEdgeData(key: String,
                                      t: TransactionEdgeData): Unit =
    put(key, t)

  override def updateTransactionEdgeData(
    key: String,
    f: TransactionEdgeData => TransactionEdgeData,
    empty: TransactionEdgeData
  ): TransactionEdgeData = update(key, f, empty)

  override def getTransactionEdgeData(
    key: String
  ): Option[TransactionEdgeData] = get[TransactionEdgeData](key)

  override def putCheckpointEdgeData(key: String, t: CheckpointEdgeData): Unit =
    put(key, t)

  override def updateCheckpointEdgeData(
    key: String,
    f: CheckpointEdgeData => CheckpointEdgeData,
    empty: CheckpointEdgeData
  ): CheckpointEdgeData = update(key, f, empty)

  override def getCheckpointEdgeData(key: String): Option[CheckpointEdgeData] =
    get[CheckpointEdgeData](key)
}

import org.constellation.LevelDB._

class LevelDBActor(dao: DAO)(implicit timeoutI: Timeout, system: ActorSystem)
    extends Actor {

  implicit val executionContext: ExecutionContext =
    system.dispatchers.lookup("db-io-dispatcher")

  val logger = Logger("LevelDB")

  def tmpDirId = file"tmp/${dao.id.medium}/db"
  def mkDB: LevelDB = LevelDB(tmpDirId)

  override def receive: Receive = active(mkDB)

  def active(db: LevelDB): Receive = {

    case RestartDB =>
      Try { db.destroy() }
        .foreach(e => logger.warn("Exception while destroying LevelDB db", e))
      context become active(mkDB)

    case DBGet(key) =>
      dao.numDBGets += 1
      val res = Try { db.getBytes(key).map { KryoSerializer.deserialize } }.toOption.flatten
      sender() ! res

    case DBPut(key, obj) =>
      dao.numDBPuts += 1
      val bytes = KryoSerializer.serializeAnyRef(obj)
      sender() ! db.putBytes(key, bytes)

    case DBUpdate(key, func, empty) =>
      dao.numDBUpdates += 1
      val res = Try { db.getBytes(key).map { KryoSerializer.deserialize } }.toOption.flatten
      val option = res.map(func)
      val obj = option.getOrElse(empty)
      val bytes = KryoSerializer.serializeAnyRef(obj)
      db.putBytes(key, bytes)
      sender() ! obj

    case DBDelete(key) =>
      if (db.contains(key)) {
        dao.numDBDeletes += 1
        sender() ! db.delete(key).isSuccess
      } else sender() ! true
  }

}
// Only need to implement kryo get / put

class LevelDB(val file: File) {
  val options = new Options()
  options.createIfMissing(true)
  Try { file.createIfNotExists(true, true) }
  val db: DB = factory.open(file.toJava, options)

  // Either
  def get(s: String) = Option(asString(db.get(bytes(s))))
  def getBytes(s: String): Option[Array[Byte]] =
    Option(db.get(bytes(s))).filter(_.nonEmpty)
  def put(k: String, v: Array[Byte]) = Try { db.put(bytes(k), v) }
  def contains(s: String): Boolean = getBytes(s).nonEmpty
  def contains[T <: ProductHash](t: T): Boolean = getBytes(t.hash).nonEmpty
  def putStr(k: String, v: String) = Try { db.put(bytes(k), bytes(v)) }
  def putBytes(k: String, v: Array[Byte]): Unit = {

    var retries = 0
    var done = false
    do {
      val attempt = Try {db.put(bytes(k), v)}
      attempt match {
        case Failure(e) => e.printStackTrace()
        case _ =>
      }
      done = attempt.isSuccess
    } while (!done && retries < 3)
  }
  def put(k: String, v: String) = Try { db.put(bytes(k), bytes(v)) }
  def putHash[T <: ProductHash, Q <: ProductHash](t: T, q: Q): Try[Unit] =
    put(t.hash, q.hash)

  // JSON
  def getAsJson[T](s: String)(implicit m: Manifest[T]): Option[T] = get(s).map {
    _.x[T]
  }
  def getHashAsJson[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] =
    get(s.hash).map { _.x[T] }
  def getRaw(s: String): String = asString(db.get(bytes(s)))
  def getSafe(s: String): Try[String] = Try { asString(db.get(bytes(s))) }
  def putJson[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] =
    put(t.hash, q.json)
  def putJson(k: String, t: AnyRef): Try[Unit] = put(k, t.json)
  def putJson[T <: ProductHash](t: T): Try[Unit] = put(t.hash, t.json)

  def kryoGet(key: String): Option[AnyRef] =
    Try { getBytes(key).map { KryoSerializer.deserialize } }.toOption.flatten
  //def kryoGetT[T <: ClassTag](key: String): Option[AnyRef] = Try{getBytes(key).map {KryoSerializer.deserialize}}.toOption.flatten

  def kryoPut(key: String, obj: AnyRef): Unit = {
    val bytes = KryoSerializer.serializeAnyRef(obj)
    putBytes(key, bytes)
  }

  /*
    // Kryo
    def getAs[T](s: String)(implicit m: Manifest[T]): Option[T] = getBytes(s).map{_.kryoExtract[T]}
    def getHashAs[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] = getBytes(s.hash).map{_.kryoExtract[T]}
    def put[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] = putBytes(t.hash, q.kryoWrite)
    def put(k: String, t: AnyRef): Try[Unit] = putBytes(k, t.kryoWrite)
    def put[T <: ProductHash](t: T): Try[Unit] = putBytes(t.hash, t.kryoWrite)
   */

  // Util

  def delete(k: String) = Try { db.delete(bytes(k)) }
  def close(): Unit = db.close()
  def destroy(): Unit = {
    close()
    file.delete(true)
  }

}
