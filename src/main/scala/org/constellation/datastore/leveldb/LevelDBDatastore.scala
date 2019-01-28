package org.constellation.datastore.leveldb

import org.constellation.DAO
import org.constellation.datastore.KVDBDatastoreImpl

/** Level database class from DAO.
  *
  * @param dao ... Data access object.
  */
class LevelDBDatastore(dao: DAO) extends KVDBDatastoreImpl {
  val kvdb = new LevelDBImpl(dao)
}
