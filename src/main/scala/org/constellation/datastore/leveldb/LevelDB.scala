package org.constellation.datastore.leveldb
import better.files._
import constellation._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.ProductHash
import org.iq80.leveldb.impl.Iq80DBFactory.{asString, bytes, factory}
import org.iq80.leveldb.{DB, Options}

import scala.util.{Failure, Try}

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