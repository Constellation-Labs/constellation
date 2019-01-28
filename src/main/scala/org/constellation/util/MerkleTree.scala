package org.constellation.util

/** Merkle node. */
case class MerkleNode(hash: String, leftChild: String, rightChild: String) {

  /** @return Concatenation of left and right child. */
  def children = Seq(leftChild, rightChild)

  /** @return Whether the input other is among this Merkle nodes children. */
  def isParentOf(other: String): Boolean = children.contains(other)

  /** @return Whether the hash from the constructor matches the left and right children. */
  def valid: Boolean = MerkleTree.merkleHashFunc(leftChild, rightChild) == hash
}

/** Merkle proof.
  *
  * @param input ... ??
  * @param nodes ... ??
  * @param root  ... ??
  * @todo document.
  */
case class MerkleProof(input: String, nodes: Seq[MerkleNode], root: String) {

  /** @return Whether this Merkle proof is valid. */
  def verify(): Boolean = {
    val childToParent = MerkleTree.childToParent(nodes)
    val parents = MerkleTree.collectParents(Seq(), childToParent(input), childToParent)
    parents.last.hash == root && parents.head.isParentOf(input) && parents.forall {
      _.valid
    }
  }
}

// doc
case class MerkleResult(inputs: Seq[String], nodes: Seq[MerkleNode]) {

  // doc
  def createProof(startingPoint: String): MerkleProof = {
    val parentMap = MerkleTree.childToParent(nodes)
    val firstParent = parentMap(startingPoint)
    MerkleProof(startingPoint, MerkleTree.collectParents(Seq(), firstParent, parentMap), nodes.last.hash)
  }
}

import constellation.SHA256Ext // TODO: move import statement up

/** Merkle tree object.
  *
  * @todo This should be changed to an actual tree structure in memory.
  * @todo Just skipping that for now.
  * @todo Either that or replace this with a pre-existing implementation
  * @todo Couldn't find any libs that were easy drop ins so just doing this for now.
  */
object MerkleTree {

  // doc
  def childToParent(nodes: Seq[MerkleNode]): Map[String, MerkleNode] = nodes.flatMap { n =>
    n.children.map {
      _ -> n
    }
  }.toMap

  // doc
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

  // doc
  def apply(hashes: List[String]): MerkleResult = {

    if (hashes.isEmpty) {
      throw new Exception("Merkle function call on empty collection of hashes")
    }
    val even = if (hashes.size % 2 != 0) hashes :+ hashes.last else hashes
    println(s"Creating Merkle tree on ${even.length} hashes")

    val zero = applyRound(even)
    MerkleResult(hashes, merkleIteration(Seq(), zero))
  }

  /** @return SHA256-hashed sum of left and right input string. */
  def merkleHashFunc(left: String, right: String): String = {
    (left + right).sha256
  }

  // doc
  def applyRound(level: Seq[String]): Seq[MerkleNode] = {
    println(s"Applying Merkle round on ${level.length} level length")
    level.grouped(2).toSeq.map {
      case Seq(l, r) =>
        MerkleNode(merkleHashFunc(l, r), l, r)
      case Seq(l) =>
        MerkleNode(merkleHashFunc(l, l), l, l)
    }
  }

  // doc
  def merkleIteration(agg: Seq[MerkleNode], currentLevel: Seq[MerkleNode]): Seq[MerkleNode] = {
    if (currentLevel.size == 1) {
      agg ++ currentLevel
    } else {
      val nextLevel = applyRound(currentLevel.map {
        _.hash
      })
      merkleIteration(agg ++ currentLevel, nextLevel)
    }
  }

} // end object MerkleTree
