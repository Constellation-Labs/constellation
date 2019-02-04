package org.constellation.datastore

import org.constellation.DAO
import org.constellation.serializer.KryoSerializer

import scala.collection.concurrent.TrieMap
import scala.util.Try

/** Documentation. */
class SimpleKVDBImpl extends KVDB {

  private val store = TrieMap[String, Array[Byte]]()

  /** Documentation. */
  override def put(key: String, obj: AnyRef): Boolean = Try{
    val bytes = KryoSerializer.serializeAnyRef(obj)
    store(key) = bytes
  }.isSuccess

  /** Documentation. */
  override def get[T <: AnyRef](key: String): Option[T] = {
    store.get(key).map{v => KryoSerializer.deserialize(v).asInstanceOf[T]}
  }

  /** Documentation. */
  override def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T = {
    val res = get(key).map{updateF}
    if (res.isEmpty) {
      put(key, empty)
      empty
    } else {
      val res2 = res.get
      put(key, res2)
      res2
    }
  }

  /** Documentation. */
  override def delete(key: String): Boolean = {
    Try{store.remove(key)}.isSuccess
  }

  /** Documentation. */
  override def restart(): Unit = {
    store.clear()
  }
}

/** Documentation. */
class SimpleKVDatastore(dao: DAO) extends KVDBDatastoreImpl {
  override val kvdb: KVDB = new SimpleKVDBImpl()
}
