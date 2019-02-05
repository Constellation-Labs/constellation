package org.constellation.datastore.leveldb

import org.constellation.DAO
import org.constellation.datastore.KVDBDatastoreImpl

class LevelDBDatastore(dao: DAO) extends KVDBDatastoreImpl {
  val kvdb = new LevelDBImpl(dao)
}
