package org.constellation


import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io._

import org.constellation.primitives.Schema.TX
import org.scalatest.FlatSpec

import scala.tools.nsc.io.{File => SFile}

class LevelDBTest extends FlatSpec {


  "LevelDB" should "create a database and run some queries and delete it" in {
    val options = new Options()
    options.createIfMissing(true)
    val file = new File("tmp" , "example")
    file.mkdir

    val db = factory.open(file, options)
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

    SFile(file).deleteRecursively()

  }

  "LevelDB wrapper" should "do same but in a convenient fashion" in {

    val file = new File("tmp" , "example")
    file.mkdir

    val ldb = new LevelDB(file)

    ldb.put("Tampa", "rocks")
    assert(ldb.getRaw("Tampa") == "rocks")
    ldb.delete("Tampa")
    assert(ldb.getRaw("Tampa") == null)

    ldb.close()
    SFile(file).deleteRecursively()

  }

  "Type serialization" should "test tx and bundle storage" in {


    val file = new File("tmp" , "example")
    file.mkdir

    val ldb = new LevelDB(file)

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
    SFile(file).deleteRecursively()

  }

}
