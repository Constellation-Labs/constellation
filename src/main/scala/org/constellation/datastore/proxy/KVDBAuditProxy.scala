package org.constellation.datastore.proxy

import better.files.File

import constellation._

import org.constellation.datastore.KVDB

/** Documentation. */
class KVDBAuditProxy(kvdb: KVDB) extends KVDB {

  private val f = File.newTemporaryFile("kvdb-audit")

  /** Documentation. */
  def timer[T](f: => T): (T, Long) = {
    val startTime = System.currentTimeMillis()
    val res = f
    val endTime = System.currentTimeMillis()
    (res, endTime - startTime)
  }

  f.createFileIfNotExists()

  /** Documentation. */
  override def put(key: String, obj: AnyRef): Boolean = {
    val (res, time) = timer { kvdb.put(key, obj) }
    f.appendLine(s"PUT $key ${obj.json} $res ${time}ms")
    res
  }

  /** Documentation. */
  override def get[T <: AnyRef](key: String): Option[T] = {
    val (res, time) = timer { kvdb.get(key) }
    f.appendLine(s"GET $key ${res.json} ${time}ms")
    res
  }

  /** Documentation. */
  override def update[T <: AnyRef](key: String,
                                   updateF: T => T,
                                   empty: T): T = {
    val (before, t1) = timer { kvdb.get(key) }
    val (res, time) = timer { kvdb.update(key, updateF, empty) }
    f.appendLine(s"UPDATE $key ${res.json} ${time}ms (BEFORE UPDATE: ${before.json} ${t1}ms)")
    res
  }

  /** Documentation. */
  override def delete(key: String): Boolean = {
    val (res, time) = timer { kvdb.delete(key) }
    f.appendLine(s"DELETE $key ${time}ms")
    res
  }

  /** Documentation. */
  override def restart(): Unit = {
    f.appendLine("RESTART")
    kvdb.restart()
  }
}

/** Documentation. */
object KVDBAuditProxy {

  /** Documentation. */
  def apply(kvdb: KVDB): KVDBAuditProxy = new KVDBAuditProxy(kvdb)
}
