package org.constellation.datastore

import org.constellation.DAO

import scala.collection.concurrent.TrieMap
import org.constellation.serializer.KryoSerializer

import scala.util.Try

class SimpleKVDBImpl extends KVDB {

  private val store = TrieMap[String, Array[Byte]]()

  override def put(key: String, obj: AnyRef): Boolean = Try{
    val bytes = KryoSerializer.serializeAnyRef(obj)
    store(key) = bytes
  }.isSuccess

  override def get[T <: AnyRef](key: String): Option[T] = {
    store.get(key).map{v => KryoSerializer.deserialize(v).asInstanceOf[T]}
  }

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

  override def delete(key: String): Boolean = {
    Try{store.remove(key)}.isSuccess
  }

  override def restart(): Unit = {
    store.clear()
  }
}


class SimpleKVDatastore(dao: DAO) extends KVDBDatastoreImpl {
  override val kvdb: KVDB = new SimpleKVDBImpl()
}
