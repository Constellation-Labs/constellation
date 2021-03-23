package org.constellation.util

import cats.effect.{ConcurrentEffect, ContextShift, IO}
import org.constellation.Fixtures
import cats.implicits._
import org.constellation.Fixtures._
import org.constellation.domain.trust.TrustDataInternal
import org.constellation.schema.transaction.Transaction
import org.constellation.serialization.KryoSerializer
import org.constellation.trust.{DataGenerator, TrustNode}
import org.constellation.util.Partitioner._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class PartitionerTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfter {

  val random = new java.util.Random()
  val randomTxs = getRandomTxs()
  val acceptableFacilBalance = 0.8
  val proposerId = Fixtures.id
  val ids = proposerId :: idSet5.toList
  val idxId = ids.zipWithIndex.toMap.map { case (k, v) => (v, k) }
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val effect: ConcurrentEffect[IO] = IO.ioConcurrentEffect(contextShift)
  val generator = new DataGenerator[IO]()
  KryoSerializer.init[IO].handleError(_ => Unit).unsafeRunSync()

  def getRandomTxs(factor: Int = 5): Set[Transaction] = idSet5.flatMap { id =>
    val destinationAddresses = idSet5.map(_.address)
    val destinationAddressDups = (0 to factor).flatMap(_ => destinationAddresses)
    destinationAddressDups.map(
      destStr => makeTransaction(id.address, destStr, random.nextLong(), getRandomElement(tempKeySet, random))
    )
  }

  def generateFullyConnectedData(numNodes: Int = 30) =
    generator.generateData(numNodes, generator.cliqueEdge(generator.seedCliqueLogic(numNodes)))

  def generateCliqueTestData(numNodes: Int = 30) =
    generator.generateData(numNodes, generator.cliqueEdge(generator.seedCliqueLogic(numNodes / 2)))

  def generateBipartiteTestData(numNodes: Int = 30) =
    generator.generateData(numNodes, generator.bipartiteEdge(generator.seedCliqueLogic(numNodes / 2)))

  "Facilitator selection" should "be deterministic" in {
    val facilitator = selectTxFacilitator(ids, randomTxs.head)
    val facilitatorDup = selectTxFacilitator(ids, randomTxs.head)
    assert(facilitator === facilitatorDup)
  }

  "Facilitators" should "not facilitate their own transactions" in {
    val facilitator = selectTxFacilitator(ids, randomTxs.head)
    assert(facilitator.address != randomTxs.head.src.address)
  }

  "Facilitator selection" should "be relatively balanced" in {
    val facilitators = randomTxs.map(tx => selectTxFacilitator(ids, tx))
    assert(facilitators.size == ids.size)
  }

  "The gossip path" should "always be shorter then the total set of node ids" in {
    val pathLengths = randomTxs.map(gossipPath(ids, _).size)
    pathLengths.foreach(println)
    assert(pathLengths.forall(_ < ids.size))
  }

  "HausdorffPartition.nerve" should "deterministically repartition random edges" in {
    val trustNodes: Seq[TrustNode] = generateFullyConnectedData(ids.size).unsafeRunSync()
    val tdi: List[TrustDataInternal] =
      trustNodes.map(tn => TrustDataInternal(idxId(tn.id), tn.edges.map(e => (idxId(e.dst), e.trust)).toMap)).toList
    val partitioner: HausdorffPartition = HausdorffPartition(tdi.tail)(tdi.head)
    val partitions: Map[Int, List[TrustDataInternal]] = partitioner.nerve
    val repartition: Map[Int, List[TrustDataInternal]] = partitioner.rePartition(tdi.tail)(partitioner.nerve)
    assert(repartition === partitions)
  }

  "HausdorffPartition.nerve" should "deterministically repartition clique edges" in {
    val trustNodes: Seq[TrustNode] = generateCliqueTestData(ids.size).unsafeRunSync()
    val tdi: List[TrustDataInternal] =
      trustNodes.map(tn => TrustDataInternal(idxId(tn.id), tn.edges.map(e => (idxId(e.dst), e.trust)).toMap)).toList
    val partitioner: HausdorffPartition = HausdorffPartition(tdi.tail)(tdi.head)
    val partitions: Map[Int, List[TrustDataInternal]] = partitioner.nerve
    val repartition: Map[Int, List[TrustDataInternal]] = partitioner.rePartition(tdi.tail)(partitioner.nerve)
    assert(repartition === partitions)
  }

  "HausdorffPartition.nerve" should "not repeat peers in partition paths" in {
    val trustNodes: Seq[TrustNode] = generateFullyConnectedData(ids.size).unsafeRunSync()
    val tdi: List[TrustDataInternal] =
      trustNodes.map(tn => TrustDataInternal(idxId(tn.id), tn.edges.map(e => (idxId(e.dst), e.trust)).toMap)).toList
    val partitioner: HausdorffPartition = HausdorffPartition(tdi.tail)(tdi.head)
    val partitions: Map[Int, List[TrustDataInternal]] = partitioner.nerve
    assert((1 until partitions.size - 2).forall { rank =>
      partitions(rank).intersect(partitions(rank + 1)).isEmpty
    })
  }

  "HausdorffPartition.nerve" should "deterministically layer bipartite cliques" in {
    val trustNodes: Seq[TrustNode] = generateBipartiteTestData(ids.size).unsafeRunSync()
    val tdi: List[TrustDataInternal] =
      trustNodes.map(tn => TrustDataInternal(idxId(tn.id), tn.edges.map(e => (idxId(e.dst), e.trust)).toMap)).toList
    val (lPartite, rPartite) = tdi.tail.zipWithIndex.partition { case (_, idx) => idx < (ids.size / 2) }
    val partitioner: HausdorffPartition = HausdorffPartition(tdi.tail)(tdi.head)
    val partitions: Map[Int, List[TrustDataInternal]] = partitioner.nerve
    val rank1Eq: Boolean = partitions(1).map(_.id).toSet == lPartite.map(_._1.id).toSet
    val rank2Eq: Boolean = partitions(2).map(_.id).toSet == rPartite.map(_._1.id).toSet
    assert(rank1Eq && rank2Eq)
  }

  "HausdorffPartition.nerve" should "contain all nodes after partitioning" in {
    val trustNodes: Seq[TrustNode] = generateBipartiteTestData(ids.size).unsafeRunSync()
    val tdi: List[TrustDataInternal] =
      trustNodes.map(tn => TrustDataInternal(idxId(tn.id), tn.edges.map(e => (idxId(e.dst), e.trust)).toMap)).toList
    val partitioner: HausdorffPartition = HausdorffPartition(tdi.tail)(tdi.head)
    val partitions: Map[Int, List[TrustDataInternal]] = partitioner.nerve
    assert(partitions.values.flatMap(_.map(_.id)).toSet === ids.toSet)
  }
}
