package org.constellation

import java.io.File

import org.constellation.util.ProductHash

import scala.util.Try
import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import constellation.SerExt
import constellation.ParseExt
import scala.tools.nsc.io.{File => SFile}

// https://doc.akka.io/docs/akka/2.5/persistence-query-leveldb.html

import constellation._

class LevelDB(val file: File) {
  val options = new Options()
  options.createIfMissing(true)
  Try{file.mkdirs}
  val db: DB = factory.open(file, options)

  // Either
  def get(s: String) = Option(asString(db.get(bytes(s))))
  def getBytes(s: String): Option[Array[Byte]] = Option(db.get(bytes(s))).filter(_.nonEmpty)
  def put(k: String, v: Array[Byte]) = Try {db.put(bytes(k), v)}
  def contains(s: String): Boolean = getBytes(s).nonEmpty
  def contains[T <: ProductHash](t: T): Boolean = getBytes(t.hash).nonEmpty
  def putStr(k: String, v: String) = Try {db.put(bytes(k), bytes(v))}
  def putBytes(k: String, v: Array[Byte]) = Try {db.put(bytes(k), v)}
  def put(k: String, v: String) = Try {db.put(bytes(k), bytes(v))}
  def putHash[T <: ProductHash, Q <: ProductHash](t: T, q: Q): Try[Unit] = put(t.hash, q.hash)

  // JSON
  def getAsJson[T](s: String)(implicit m: Manifest[T]): Option[T] = get(s).map{_.x[T]}
  def getHashAsJson[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] = get(s.hash).map{_.x[T]}
  def getRaw(s: String): String = asString(db.get(bytes(s)))
  def getSafe(s: String): Try[String] = Try{asString(db.get(bytes(s)))}
  def putJson[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] = put(t.hash, q.json)
  def putJson(k: String, t: AnyRef): Try[Unit] = put(k, t.json)
  def putJson[T <: ProductHash](t: T): Try[Unit] = put(t.hash, t.json)

  // Kryo
  def getAs[T](s: String)(implicit m: Manifest[T]): Option[T] = getBytes(s).map{_.kryoExtract[T]}
  def getHashAs[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] = getBytes(s.hash).map{_.kryoExtract[T]}
  def put[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] = putBytes(t.hash, q.kryoWrite)
  def put(k: String, t: AnyRef): Try[Unit] = putBytes(k, t.kryoWrite)
  def put[T <: ProductHash](t: T): Try[Unit] = putBytes(t.hash, t.kryoWrite)


  // Util

  def delete(k: String) = Try{db.delete(bytes(k))}
  def close(): Unit = db.close()
  def destroy(): Unit = {
    close()
    SFile(file).deleteRecursively()
  }

}