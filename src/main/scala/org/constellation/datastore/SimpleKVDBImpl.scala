package org.constellation.datastore

import org.constellation.DAO
import org.constellation.serializer.KryoSerializer

import scala.collection.concurrent.TrieMap
import scala.util.Try

/** Key-value database implementation class. */
class SimpleKVDBImpl extends KVDB {

  private val store = TrieMap[String, Array[Byte]]()

  /** Put method. */
  override def put(key: String, obj: AnyRef): Boolean = Try {
    val bytes = KryoSerializer.serializeAnyRef(obj)
    store(key) = bytes
  }.isSuccess

  /** Getter for entry corresponding to input key. */
  override def get[T <: AnyRef](key: String): Option[T] = {
    store.get(key).map { v => KryoSerializer.deserialize(v).asInstanceOf[T] }
  }

  /** Updates entry corresponding to input key. */
  override def update[T <: AnyRef](key: String, updateF: T => T, empty: T): T = {
    val res = get(key).map {
      updateF
    }
    if (res.isEmpty) {
      put(key, empty)
      empty
    } else {
      val res2 = res.get
      put(key, res2)
      res2
    }
  }

  /** Deletes entry corresponding to input key. */
  override def delete(key: String): Boolean = {
    Try {
      store.remove(key)
    }.isSuccess
  }

  /** Resets store. */
  override def restart(): Unit = {
    store.clear()
  }

} // end SimpleKVDBImpl

/** Key-value database class. */
class SimpleKVDatastore(dao: DAO) extends KVDBDatastoreImpl {
  override val kvdb: KVDB = new SimpleKVDBImpl()
}
