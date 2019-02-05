package org.constellation.util


case class MerkleNode(hash: String, leftChild: String, rightChild: String) {
  def children = Seq(leftChild, rightChild)
  def isParentOf(other: String): Boolean = children.contains(other)
  def valid: Boolean = MerkleTree.merkleHashFunc(leftChild, rightChild) == hash
}


case class MerkleProof(input: String, nodes: Seq[MerkleNode], root: String) {

  def verify(): Boolean = {
    val childToParent = MerkleTree.childToParent(nodes)
    val parents = MerkleTree.collectParents(Seq(), childToParent(input), childToParent)
    parents.last.hash == root && parents.head.isParentOf(input) && parents.forall{_.valid}
  }
}

case class MerkleResult(inputs: Seq[String], nodes: Seq[MerkleNode]) {

  def createProof(startingPoint: String): MerkleProof = {
    val parentMap = MerkleTree.childToParent(nodes)
    val firstParent = parentMap(startingPoint)
    MerkleProof(startingPoint, MerkleTree.collectParents(Seq(), firstParent, parentMap), nodes.last.hash)
  }
}

import com.typesafe.scalalogging.StrictLogging
import constellation.SHA256Ext


// This should be changed to an actual tree structure in memory. Just skipping that for now
// Either that or replace this with a pre-existing implementation
// Couldn't find any libs that were easy drop ins so just doing this for now
object MerkleTree extends StrictLogging {

  def childToParent(nodes: Seq[MerkleNode]): Map[String, MerkleNode] = nodes.flatMap{ n =>
    n.children.map{
      _ -> n
    }
  }.toMap

  def collectParents(
                       parents: Seq[MerkleNode],
                       activeNode: MerkleNode,
                       childToParent: Map[String, MerkleNode]
                     ): Seq[MerkleNode] = {

    val newParents = parents :+ activeNode
    childToParent.get(activeNode.hash) match {
      case None =>
        newParents
      case Some(parent) =>
        collectParents(newParents, parent, childToParent)
    }
  }

  def apply(hashes: List[String]): MerkleResult = {

    if (hashes.isEmpty) {
      throw new Exception("Merkle function call on empty collection of hashes")
    }
    val even = if (hashes.size % 2 != 0) hashes :+ hashes.last else hashes
    logger.debug(s"Creating Merkle tree on ${even.length} hashes")

    val zero = applyRound(even)
    MerkleResult(hashes, merkleIteration(Seq(), zero))
  }

  def merkleHashFunc(left: String, right: String): String = {
    (left + right).sha256
  }

  def applyRound(level: Seq[String]): Seq[MerkleNode] = {
    logger.debug(s"Applying Merkle round on ${level.length} level length")
    level.grouped(2).toSeq.map{
      case Seq(l, r) =>
        MerkleNode(merkleHashFunc(l,r), l, r)
      case Seq(l) =>
        MerkleNode(merkleHashFunc(l,l), l, l)
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

}