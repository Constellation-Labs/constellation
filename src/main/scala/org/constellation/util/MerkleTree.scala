package org.constellation.util

import scala.annotation.tailrec

case class MerkleNode(hash: String, leftChild: String, rightChild: String) {
  def children = Seq(leftChild, rightChild)
  def isParentOf(other: String): Boolean = children.contains(other)
}

case class MerkleResult(inputs: Seq[String], nodes: Seq[MerkleNode]) {

  def createProof(inputIndex: Int) = {

    val parentMap = nodes.flatMap{ n =>
      n.children.map{
        _ => n.hash
      }
    }.groupBy()
    val startingPoint = inputs(inputIndex)
    val zeroChild = nodes.filter(n => n.isParentOf(startingPoint)).head

  }
}

import constellation.SHA256Ext


object MerkleTree {

  def collectParents(
                       remainingNodes: Seq[MerkleNode],
                       children: Seq[MerkleNode],
                       activeNode: MerkleNode
                     ) = {
    if (remainingNodes.isEmpty) {
      children :+ activeNode
    } else {
      val nextChild = remainingNodes.filter(n => n.isParentOf(startingPoint)).head
    }
  }

  def apply(hashes: Seq[String]): MerkleResult = {
    if (hashes.isEmpty) {
      throw new Exception("Merkle function call on empty collection of hashes")
    }
    val even = if (hashes.size % 2 != 0) hashes :+ hashes.last else hashes
    val zero = applyRound(even)
    MerkleResult(hashes, merkleIteration(Seq(), zero))
  }

  def applyRound(level: Seq[String]): Seq[MerkleNode] = {
    level.grouped(2).toSeq.map{
      case Seq(l, r) =>
        MerkleNode((l + r).sha256, l, r)
    }
  }

  def merkleIteration(agg: Seq[MerkleNode], currentLevel: Seq[MerkleNode]): Seq[MerkleNode] = {
    if (currentLevel.size == 1) {
      agg ++ currentLevel
    } else {
      val nextLevel = applyRound(currentLevel.map{_.hash})
      merkleIteration(agg ++ currentLevel, nextLevel)
    }
  }

  def main(args: Array[String]): Unit = {

    val testHash = Seq.tabulate(25){i => s"e_$i"}.map{_.sha256}

    val result = apply(testHash)




  }

}