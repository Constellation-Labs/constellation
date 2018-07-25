package org.constellation


import better.files.File
import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import org.scalatest.FlatSpec

class LevelDBTest extends FlatSpec {


  "LevelDB" should "create a database and run some queries and delete it" in {
    val options = new Options()
    options.createIfMissing(true)
    val file = File("tmp" , "example")
    file.createIfNotExists(true, true)

    val db = factory.open(file.toJava, options)
    try {
      db.put(bytes("Tampa"), bytes("rocks"))
      val value = asString(db.get(bytes("Tampa")))
      assert(value == "rocks")
      db.delete(bytes("Tampa"))
      val value2 = asString(db.get(bytes("Tampa")))
      assert(value2 == null)
    } finally {
      // Make sure you close the db to shutdown the
      // database and avoid resource leaks.
      db.close()
    }

    file.delete(true)

  }

  "LevelDB wrapper" should "do same but in a convenient fashion" in {

    val file = File("tmp" , "example")
    file.createIfNotExists(true, true)

    val ldb = LevelDB(file)

    ldb.put("Tampa", "rocks")
    assert(ldb.getRaw("Tampa") == "rocks")
    ldb.delete("Tampa")
    assert(ldb.getRaw("Tampa") == null)

    ldb.close()
    file.delete(true)

  }

  "Type serialization" should "test tx and bundle storage" in {


    val file = File("tmp" , "example")
    file.createIfNotExists(true, true)

    val ldb = LevelDB(file)

    ldb.put("Tampa", "rocks")
    assert(ldb.getRaw("Tampa") == "rocks")
    ldb.delete("Tampa")
    assert(ldb.getRaw("Tampa") == null)
/*

    val tx = Fixtures.randomTransactions.head
    import constellation._
    val write = tx.kryoWrite
    println("write length " + write.size)
    ldb.put("asdf", write)
    val res = ldb.getBytes("asdf").get
    println(res.length)
    println(res.kryoRead.asInstanceOf[TX])

    ldb.put(tx.hash, write)
    val tx2 = ldb.getAs[TX](tx.hash).get
    //println(tx)
    //println(tx2)
    assert(tx == tx2)
*/

    ldb.close()
    file.delete(true)

  }

}
