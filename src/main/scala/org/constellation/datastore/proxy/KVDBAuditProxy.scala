package org.constellation.datastore.proxy
import better.files.File
import org.constellation.datastore.KVDB

import constellation._

class KVDBAuditProxy(kvdb: KVDB) extends KVDB {

  private val f = File.newTemporaryFile("kvdb-audit")

  f.createFileIfNotExists()

  override def put(key: String, obj: AnyRef): Boolean = {
    val res = kvdb.put(key, obj)
    f.appendLine(s"PUT $key ${obj.json} $res")
    res
  }

  override def get[T <: AnyRef](key: String): Option[T] = {
    val res = kvdb.get(key)
    f.appendLine(s"GET $key ${res.json}")
    res
  }

  override def update[T <: AnyRef](key: String,
                                   updateF: T => T,
                                   empty: T): T = {
    val before = kvdb.get(key)
    val res = kvdb.update(key, updateF, empty)
    f.appendLine(s"UPDATE $key ${res.json} (BEFORE UPDATE: ${before.json})")
    res
  }

  override def delete(key: String): Boolean = {
    val res = kvdb.delete(key)
    f.appendLine(s"DELETE $key")
    res
  }

  override def restart(): Unit = {
    f.appendLine("RESTART")
    kvdb.restart()
  }
}
object KVDBAuditProxy {
  def apply(kvdb: KVDB): KVDBAuditProxy = new KVDBAuditProxy(kvdb)
}
