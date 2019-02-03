package org.constellation.datastore.leveldb
import better.files._
import constellation._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.Signable
import org.iq80.leveldb.impl.Iq80DBFactory.{asString, bytes, factory}
import org.iq80.leveldb.{DB, Options}

import scala.util.{Failure, Try}

/** Documentation. */
class LevelDB(val file: File) {
  val options = new Options()
  options.createIfMissing(true)
  Try { file.createIfNotExists(true, true) }
  val db: DB = factory.open(file.toJava, options)

  // Either

  /** Documentation. */
  def get(s: String) = Option(asString(db.get(bytes(s))))

  /** Documentation. */
  def getBytes(s: String): Option[Array[Byte]] =
    Option(db.get(bytes(s))).filter(_.nonEmpty)

  /** Documentation. */
  def put(k: String, v: Array[Byte]) = Try { db.put(bytes(k), v) }

  /** Documentation. */
  def contains(s: String): Boolean = getBytes(s).nonEmpty

  /** Documentation. */
  def contains[T <: Signable](t: T): Boolean = getBytes(t.hash).nonEmpty

  /** Documentation. */
  def putStr(k: String, v: String) = Try { db.put(bytes(k), bytes(v)) }

  /** Documentation. */
  def putBytes(k: String, v: Array[Byte]): Boolean = {

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
    done
  }

  /** Documentation. */
  def put(k: String, v: String) = Try { db.put(bytes(k), bytes(v)) }

  /** Documentation. */
  def putHash[T <: Signable, Q <: Signable](t: T, q: Q): Try[Unit] =
    put(t.hash, q.hash)

  // JSON

  /** Documentation. */
  def getAsJson[T](s: String)(implicit m: Manifest[T]): Option[T] = get(s).map {
    _.x[T]
  }

  /** Documentation. */
  def getHashAsJson[T](s: Signable)(implicit m: Manifest[T]): Option[T] =
    get(s.hash).map { _.x[T] }

  /** Documentation. */
  def getRaw(s: String): String = asString(db.get(bytes(s)))

  /** Documentation. */
  def getSafe(s: String): Try[String] = Try { asString(db.get(bytes(s))) }

  /** Documentation. */
  def putJson[T <: Signable, Q <: AnyRef](t: T, q: Q): Try[Unit] =
    put(t.hash, q.json)

  /** Documentation. */
  def putJson(k: String, t: AnyRef): Try[Unit] = put(k, t.json)

  /** Documentation. */
  def putJson[T <: Signable](t: T): Try[Unit] = put(t.hash, t.json)

  /** Documentation. */
  def kryoGet(key: String): Option[AnyRef] =
    Try { getBytes(key).map { KryoSerializer.deserialize } }.toOption.flatten
  //def kryoGetT[T <: ClassTag](key: String): Option[AnyRef] = Try{getBytes(key).map {KryoSerializer.deserialize}}.toOption.flatten

  /** Documentation. */
  def kryoPut(key: String, obj: AnyRef): Unit = {
    val bytes = KryoSerializer.serializeAnyRef(obj)
    putBytes(key, bytes)
  }

  /*
    // Kryo

    /** Documentation. */
    def getAs[T](s: String)(implicit m: Manifest[T]): Option[T] = getBytes(s).map{_.kryoExtract[T]}

    /** Documentation. */
    def getHashAs[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] = getBytes(s.hash).map{_.kryoExtract[T]}

    /** Documentation. */
    def put[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] = putBytes(t.hash, q.kryoWrite)

    /** Documentation. */
    def put(k: String, t: AnyRef): Try[Unit] = putBytes(k, t.kryoWrite)

    /** Documentation. */
    def put[T <: ProductHash](t: T): Try[Unit] = putBytes(t.hash, t.kryoWrite)
   */

  // Util

  /** Documentation. */
  def delete(k: String) = Try { db.delete(bytes(k)) }

  /** Documentation. */
  def close(): Unit = db.close()

  /** Documentation. */
  def destroy(): Unit = {
    close()
    file.delete(true)
  }

}

/** Documentation. */
object LevelDB {

  case object RestartDB

  /** Documentation. */
  case class DBGet(key: String)

  /** Documentation. */
  case class DBPut(key: String, obj: AnyRef)

  /** Documentation. */
  case class DBDelete(key: String)

  /** Documentation. */
  case class DBUpdate[T <: AnyRef](key: String, f: T => T, empty: T)

  /** Documentation. */
  def apply(file: File) = {
    new LevelDB(file)
  }

}
