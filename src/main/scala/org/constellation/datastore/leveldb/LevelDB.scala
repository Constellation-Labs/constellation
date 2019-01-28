package org.constellation.datastore.leveldb

import better.files._

import constellation._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.ProductHash

import org.iq80.leveldb.impl.Iq80DBFactory.{asString, bytes, factory}
import org.iq80.leveldb.{DB, Options}
import scala.util.{Failure, Try}

/** Level database class. */
class LevelDB(val file: File) {

  val options = new Options()

  options.createIfMissing(true)

  Try {
    file.createIfNotExists(true, true)
  }

  val db: DB = factory.open(file.toJava, options)

  /** Getter as string. */
  def get(s: String) = Option(asString(db.get(bytes(s))))

  /** Getter as byte array. */
  def getBytes(s: String): Option[Array[Byte]] =
    Option(db.get(bytes(s))).filter(_.nonEmpty)

  /** Put method for a byte array. */
  def put(k: String, v: Array[Byte]) = Try {
    db.put(bytes(k), v)
  }

  /** @return Whether there is an entry for the input key. */
  def contains(s: String): Boolean = getBytes(s).nonEmpty

  /** @return Whether there is an entry for the input hashed key. */
  def contains[T <: ProductHash](t: T): Boolean = getBytes(t.hash).nonEmpty

  /** Put method for a string. */
  def putStr(k: String, v: String) = Try {
    db.put(bytes(k), bytes(v))
  }

  /** Put method for a byte array. */
  def putBytes(k: String, v: Array[Byte]): Boolean = {

    var retries = 0

    var done = false

    do {
      val attempt = Try {
        db.put(bytes(k), v)
      }
      attempt match {
        case Failure(e) => e.printStackTrace()
        case _ =>
      }
      done = attempt.isSuccess
    } while (!done && retries < 3)

    done
  }

  /** Put method for a string. */
  def put(k: String, v: String) = Try {
    db.put(bytes(k), bytes(v))
  }

  /** Put method for a string under the hashed key. */
  def putHash[T <: ProductHash, Q <: ProductHash](t: T, q: Q): Try[Unit] =
    put(t.hash, q.hash)

  /** Getter for JSON data for key. */
  def getAsJson[T](s: String)(implicit m: Manifest[T]): Option[T] = get(s).map {
    _.x[T]
  }

  /** Getter for JSON data for hashed key. */
  def getHashAsJson[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] =
    get(s.hash).map {
      _.x[T]
    }

  /** Getter. */
  def getRaw(s: String): String = asString(db.get(bytes(s)))

  // doc
  def getSafe(s: String): Try[String] = Try {
    asString(db.get(bytes(s)))
  }

  // doc
  def putJson[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] =
    put(t.hash, q.json)

  // doc
  def putJson(k: String, t: AnyRef): Try[Unit] = put(k, t.json)

  // doc
  def putJson[T <: ProductHash](t: T): Try[Unit] = put(t.hash, t.json)

  // doc
  def kryoGet(key: String): Option[AnyRef] =
    Try {
      getBytes(key).map {
        KryoSerializer.deserialize
      }
    }.toOption.flatten

  /* // tmp comment
  def kryoGetT[T <: ClassTag](key: String): Option[AnyRef] =
    Try{getBytes(key).map {KryoSerializer.deserialize}}.toOption.flatten
  */

  // doc
  def kryoPut(key: String, obj: AnyRef): Unit = {
    val bytes = KryoSerializer.serializeAnyRef(obj)
    putBytes(key, bytes)
  }

  /* // tmp comment
    // doc
    def getAs[T](s: String)(implicit m: Manifest[T]): Option[T] = getBytes(s).map{_.kryoExtract[T]}

    // doc
    def getHashAs[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] = getBytes(s.hash).map{_.kryoExtract[T]}

    // doc
    def put[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] = putBytes(t.hash, q.kryoWrite)

    // doc
    def put(k: String, t: AnyRef): Try[Unit] = putBytes(k, t.kryoWrite)

    // doc
    def put[T <: ProductHash](t: T): Try[Unit] = putBytes(t.hash, t.kryoWrite)
   */

  // Util // tmp comment

  // doc
  def delete(k: String) = Try {
    db.delete(bytes(k))
  }

  // doc
  def close(): Unit = db.close()

  // doc
  def destroy(): Unit = {
    close()
    file.delete(true)
  }

} // end class LevelDB

/** Level database companion object. */
object LevelDB {

  case object RestartDB

  // doc
  case class DBGet(key: String)

  // doc
  case class DBPut(key: String, obj: AnyRef)

  // doc
  case class DBDelete(key: String)

  // doc
  case class DBUpdate[T <: AnyRef](key: String, f: T => T, empty: T)

  // doc
  def apply(file: File) = {
    new LevelDB(file)
  }

} // end object LevelDB
