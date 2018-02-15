package org.constellation


import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io._

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

}
