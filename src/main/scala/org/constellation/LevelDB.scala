package org.constellation

import java.io.File

import scala.util.Try

import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._

// https://doc.akka.io/docs/akka/2.5/persistence-query-leveldb.html

class LevelDB(val file: File) {
  val options = new Options()
  options.createIfMissing(true)
  file.mkdir
  val db: DB = factory.open(file, options)

  def getSafe(s: String): Try[String] =
    Try {asString(db.get(bytes(s)))}

  def get(s: String) = Option(asString(db.get(bytes(s))))

  def getRaw(s: String): String = asString(db.get(bytes(s)))

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

}