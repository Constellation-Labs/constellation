package org.constellation.trust
import org.constellation.trust.SelfAvoidingWalk.runWalk
import org.scalatest.FlatSpec

import scala.util.Random

class TrustTest extends FlatSpec {

  "Single malicious edge" should "weight good nodes over bad" in {

    val goodNodes = Seq.tabulate(10){ i =>

      // Change edges to Map[Dst, TrustInfo]
      val edgeIds = Random.shuffle(Seq.tabulate(10) { identity}).take(Random.nextInt(3) + 5)

      TrustNode(i, 0D, 0D, edgeIds.map{ dst =>
        TrustEdge(i, dst, Random.nextDouble())
      })
    }

    val badNodes = Seq.tabulate(10){ i =>
      val iOffset = 10 + i
      val edgeIds = Random.shuffle(Seq.tabulate(10) { _ + 10}).take(Random.nextInt(3) + 5)

      TrustNode(iOffset, 0D, 0D, edgeIds.map{ dst =>
        TrustEdge(i, dst, Random.nextDouble())
      })
    }

    val updatedGoodNodesWithBadEdge = goodNodes.tail :+
      goodNodes.head.copy(edges = goodNodes.head.edges :+ TrustEdge(goodNodes.head.id, badNodes.head.id, 0.2D))

    val secondNode = goodNodes.tail.head

    val weightedEdges = runWalk(secondNode.id, updatedGoodNodesWithBadEdge ++ badNodes)

    val good = weightedEdges.slice(0, 10)
    val bad = weightedEdges.slice(10, 20)

    println("Good sum", good.sum)
    println("Bad sum", bad.sum)

    assert(good.sum > bad.sum)


  }

}
