package org.constellation.checkpoint

import org.constellation.primitives.Schema.CheckpointCache

import scala.annotation.tailrec

object TopologicalSort {

  def sortBlocksTopologically(blocks: List[CheckpointCache]): List[CheckpointCache] = {
    // I need to get blocks back ;/ Not sure if it is efficient enough
    val blocksMap = blocks.map(b => b.checkpointBlock.soeHash -> b).toMap

    val edges = blocks.flatMap { b =>
      val dst = b.checkpointBlock.soeHash
      b.checkpointBlock.parentSOEHashes.map(src => (src, dst))
    }

    val sorted = TopologicalSort.sortTopologically(edges)

    sorted.map(blocksMap(_)).toList
  }

  def sortTopologically[A](edges: Traversable[(A, A)]): Seq[A] = {
    @tailrec
    def sortTopologically(inEdges: Map[A, Set[A]], alreadySorted: Seq[A]): Seq[A] = {
      val (verticesWithoutParentEdges, verticesWithParentEdges) = inEdges.partition {
        case (_, parentEdges) => parentEdges.isEmpty
      }

      if (verticesWithoutParentEdges.isEmpty) {
        if (verticesWithParentEdges.isEmpty) alreadySorted else sys.error(verticesWithParentEdges.toString)
      } else {
        val newAlreadySorted = verticesWithoutParentEdges.keys
        sortTopologically(verticesWithParentEdges.mapValues {
          _ -- newAlreadySorted
        }, alreadySorted ++ newAlreadySorted)
      }
    }

    val vertexToParentEdges = edges.foldLeft(Map[A, Set[A]]()) { (edges, e) =>
      val (src, dst) = e
      val srcInnerEdges = edges.getOrElse(src, Set())
      val dstInnerEdges = edges.getOrElse(dst, Set())
      edges + (src -> srcInnerEdges) + (dst -> (dstInnerEdges + src))
    }

    sortTopologically(vertexToParentEdges, Seq())
  }

}
