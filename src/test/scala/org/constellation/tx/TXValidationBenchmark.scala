package org.constellation.tx

import java.security.KeyPair
import java.util.concurrent.TimeUnit
import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import better.files.{File, _}
import com.typesafe.scalalogging.Logger

import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.crypto.KeyUtils._
import org.constellation.datastore.leveldb.LevelDB
import org.constellation.datastore.leveldb.LevelDB.{DBGet, DBPut}
import org.constellation.primitives.Schema
import org.constellation.primitives.Schema.AddressCacheData
import org.constellation.util.SignHelp
import org.constellation.{DAO, LevelDBActor}
import org.scalatest.FlatSpec

import scala.util.Try

// doc
class TXValidationBenchmark extends FlatSpec {
  val logger = Logger("TXValidationBenchmark")

  val kp: KeyPair = KeyUtils.makeKeyPair()
  val kp1: KeyPair = KeyUtils.makeKeyPair()
  val tx = SignHelp.createTransaction(kp.address.address, kp1.address.address, 1L, kp)

  val batchSize = 100

  // System dependent, this is a large enough buffer though
  "Timing tx signature" should "validate 10k transaction signatures under 30s" in {

    val seq = Seq.fill(batchSize)(tx)

    val parSeq = seq.par
    val t0 = System.nanoTime()
    parSeq.map(_.validSrcSignature)
    val t1 = System.nanoTime()
    val delta = (t1 - t0) / 1e6.toLong
    logger.debug(delta.toString)
    // assert(delta < 30000)

  }

  "Timing tx signature direct" should "validate 10k transaction signatures under 30s from bytes" in {

    val batch = tx.edge.signedObservationEdge.signatureBatch
    val sig = batch.signatures.head
    val pkey = sig.publicKey

    val hashBytes = batch.hash.getBytes()
    val signatureBytes = hex2bytes(sig.signature)
    assert(KeyUtils.verifySignature(hashBytes, signatureBytes)(pkey))

    val seq2 = Seq.fill(batchSize)(0).par
    val t0a = System.nanoTime()
    seq2.map(_ => KeyUtils.verifySignature(hashBytes, signatureBytes)(pkey))
    val t1a = System.nanoTime()
    val delta2 = (t1a - t0a) / 1e6.toLong
    logger.debug(delta2.toString)
    //  assert(delta2 < 30000)

  }

  "Timing tx with direct LDB" should "validate 10k transaction signatures and LDB direct balance calls under 60s" in {

    val tmpDir = "tmp"
    val ldbFile = file"tmp/db"
    Try {
      File(tmpDir).delete()
    }

    val seq = Seq.fill(batchSize)(tx)

    val ldb = LevelDB(ldbFile)

    ldb.kryoPut(tx.src.address, AddressCacheData(1e15.toLong, 1e15.toLong))

    val parSeq = seq.par
    val t0 = System.nanoTime()
    parSeq.map { t =>
      t.validSrcSignature && ldb.kryoGet(t.src.address).asInstanceOf[Option[AddressCacheData]].get.balance >= t.amount
    }
    val t1 = System.nanoTime()
    val delta = (t1 - t0) / 1e6.toLong
    logger.debug(delta.toString)
    // assert(delta < 60000)

    Try {
      File(tmpDir).delete()
    }

  }

  "Timing tx with actor LDB" should "validate 10k transaction signatures and LDB actor balance calls under 60s" in {

    val tmpDir = "tmp"
    val ldbFile = file"tmp/db"
    Try {
      File(tmpDir).delete()
    }

    val seq = Seq.fill(batchSize)(tx)

    implicit val as: ActorSystem = ActorSystem("test")

    val dao = new DAO()

    dao.keyPair = kp

    val ldb = as.actorOf(Props(new LevelDBActor(dao)), "db")

    ldb ! DBPut(tx.src.address, AddressCacheData(1e15.toLong, 1e15.toLong))

    Thread.sleep(500)

    val parSeq = seq.par
    val t0 = System.nanoTime()
    parSeq.map { t =>
      t.validSrcSignature && (ldb ? DBGet(t.src.address)).mapTo[Option[AddressCacheData]].get().get.balance >= t.amount
    }
    val t1 = System.nanoTime()
    val delta = (t1 - t0) / 1e6.toLong
    logger.debug(delta.toString)
    // assert(delta < 60000)

    Try {
      File(tmpDir).delete()
    }
  }

} // end TXValidationBenchmark class
