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

class LevelDB(val file: File) {
  val options = new Options()
  options.createIfMissing(true)
  Try{file.mkdirs}
  val db: DB = factory.open(file, options)

  def getSafe(s: String): Try[String] =
    Try {asString(db.get(bytes(s)))}

  def get(s: String) = Option(asString(db.get(bytes(s))))

  def getAs[T](s: String)(implicit m: Manifest[T]): Option[T] = get(s).map{_.x[T]}
  def getHashAs[T](s: ProductHash)(implicit m: Manifest[T]): Option[T] = get(s.hash).map{_.x[T]}

  def getRaw(s: String): String = asString(db.get(bytes(s)))

  def contains(s: String): Boolean = get(s).nonEmpty

  def contains[T <: ProductHash](t: T): Boolean = {
    get(t.hash).nonEmpty
  }


  def putHash[T <: ProductHash, Q <: ProductHash](t: T, q: Q): Try[Unit] = {
    put(t.hash, q.hash)
  }

  def put[T <: ProductHash, Q <: AnyRef](t: T, q: Q): Try[Unit] = {
    put(t.hash, q.json)
  }

  def put(k: String, t: AnyRef): Try[Unit] = {
    put(k, t.json)
  }


  def put[T <: ProductHash](t: T): Try[Unit] = {
    put(t.hash, t.json)
  }

  def put(k: String, v: String) = Try {
    db.put(bytes(k), bytes(v))
  }

  def put(k: String, v: Array[Byte]) = Try {
    db.put(bytes(k), v)
  }

  def delete(k: String) = Try{
    db.delete(bytes(k))
  }

  def close(): Unit = db.close()

  def destroy(): Unit = {
    close()
    SFile(file).deleteRecursively()
  }

}