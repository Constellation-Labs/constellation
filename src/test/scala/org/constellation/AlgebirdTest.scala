/*
package org.constellation

import org.scalatest.FlatSpec
import com.twitter.algebird._
import scala.collection.immutable
import scala.util.Random

import constellation._
import org.constellation.primitives.Schema.{TX, TXData}

/** Documentation. */
class AlgebirdTest extends FlatSpec {

  private val randomTX = Seq.fill(30) {
    val kp = makeKeyPair()
    val kp2 = makeKeyPair()
    createTransactionSafe(kp.address.address, kp2.address.address, 1L, kp)
  }

  private val randomIds = randomTX.map{_.hash}

  private val txHashStr = randomTX.map{_._1.hashSignature.signedHash}
  private val txHash = randomTX.map{_._1.hashSignature.signedHash}.map {_.getBytes}

  "HLL" should "estimate hash unions" in {

    val hllMonoid = new HyperLogLogMonoid(bits = 12)
    val hllAll = txHash.map {
      hllMonoid.create
    }
    val approx = hllMonoid.sizeOf(hllMonoid.sum(hllAll))
    assert(approx.estimate > 20)

  }

  "SketchMap" should "estimate id frequencies per tx hash with HLL values" in {

    /** Documentation. */
    class HLLOrdering extends Ordering[HLL] {

      /** Documentation. */
      override def compare(x: HLL, y: HLL): Int = {
        x.approximateSize.estimate.compare(y.approximateSize.estimate)
      }
    }

    implicit val hllOrder: HLLOrdering = new HLLOrdering()
    implicit val hllMonoid: HyperLogLogMonoid = new HyperLogLogMonoid(bits = 12)

    val DELTA = 1E-8

    val EPS = 0.001

    val SEED = 1

    val HEAVY_HITTERS_COUNT = 10

    /** Documentation. */
    implicit def string2Bytes(i: String): Array[Byte] = i.toCharArray.map(_.toByte)

    val PARAMS = SketchMapParams[String](SEED, EPS, DELTA, HEAVY_HITTERS_COUNT)

    val MONOID = SketchMap.monoid[String, HLL](PARAMS)

    val dataI = List.fill(30) {
      val tx = randomTX(Random.nextInt(randomTX.length))
      val ids = Seq.fill(Random.nextInt(10) + 5) {
        randomIds(Random.nextInt(randomIds.length))
      }.map {
        _.getBytes
      }
      tx._1.hashSignature.signedHash -> ids
    }

    val actualFrequencies = dataI.groupBy(_._1).map {
      case (h, kvs) =>
        val ids = kvs.flatMap { z => z._2.map { zz => new String(zz) } }.distinct.size
        h -> ids
    }.toMap

    val data = dataI.map {
      case (hash, ids) =>
        val hllAll = ids.map {
          hllMonoid.create
        }
        val hll = hllMonoid.sum(hllAll)
        hash -> hll
    }

    val sm = MONOID.create(data)

    val approxFreq = dataI.map {
      _._1
    }.distinct.map { h =>
      h -> MONOID.frequency(sm, h).approximateSize -> actualFrequencies(h)
    }

    println(actualFrequencies.size)
    println(approxFreq.length)

    approxFreq.foreach {
      println
    }

    approxFreq.foreach {
      case ((h, apr), actual) =>
        assert(Math.abs(apr.estimate - actual) < 5)
    }

  }

  "MinHasher" should "find similar transaction hash sets without n^2 comparisons" in {

    val mh32 = new MinHasher32(0.4, 64)

    val dataI: immutable.Seq[Set[String]] = List.fill(10) {
      val txs = Seq.fill(Random.nextInt(10) + 5) {
        randomTX(Random.nextInt(randomTX.length))
      }
      txs.map(_._1.hashSignature.signedHash).toSet
    }

    println(dataI.length)

    val hashed = dataI.map { d =>
      val init = d.toSeq.map { s => mh32.init(s) }
      val sig = mh32.combineAll(init)
      val b = mh32.buckets(sig)
      (d, sig, b)
    }

    println(hashed.length)

    val seq = hashed.combinations(2).toSeq
    println(seq.length)
    val bucketIntersections = seq.map {
      case List((s1, m1, b1), (s2, m2, b2)) =>
        val j1 = s1.intersect(s2).size.toDouble / s1.union(s2).size.toDouble
        val jm = mh32.similarity(m1, m2)
    //    assert(Math.abs(j1 - jm) < 0.3)
        val ib = b1.intersect(b2).size
        (j1, jm, ib)
    }

    bucketIntersections.foreach{println}

    assert(bucketIntersections.exists(_._3 > 0))

    val bucketGroups = hashed.flatMap{ case (s, m, b) =>
        b.zipWithIndex.map{ k => k -> (s, m)}
      }.groupBy{_._1}.map{ case (k, vs) => k -> vs.map{_._2}.size}

    assert(bucketGroups.exists{_._2 > 1})

  }
}
*/
