package org.constellation.domain.healthcheck

import cats.syntax.all._
import org.constellation.domain.healthcheck.ReconciliationRound.{NodeAligned, NodeInconsistentlySeenAsOnlineOrOffline, NodeNotPresentOnAllNodes, NodeReconciliationData, calculateClusterAlignment}
import org.constellation.schema.Id
import org.constellation.schema.NodeState.{DownloadInProgress, Leaving, Offline, PendingDownload, Ready, ReadyForDownload, SnapshotCreation}
import org.constellation.schema.consensus.RoundId
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ReconciliationRoundTest extends AnyFreeSpec with Matchers {

  val peerA = Id("A")
  val peerB = Id("B")
  val peerC = Id("C")
  val peerD = Id("D")

  "calculateClusterAlignment" - {
    "should indicate that the cluster is aligned" - {
      "when all peers are seen and in online state" in {
        val peersClusterState = Map(
          peerA -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, DownloadInProgress.some, None, Some(100L))
          ),
          peerB -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, SnapshotCreation.some, None, Some(100L))
          ),
          peerC -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, Ready.some, None, Some(100L)),
          )
        )

        val expected = Map(
          peerA -> List(NodeAligned),
          peerB -> List(NodeAligned),
          peerC -> List(NodeAligned)
        )

        val result = calculateClusterAlignment(peersClusterState)

        result shouldBe expected
      }

      "when nodes that see online peers in offline state are still running a consensus round for these nodes" in {
        val peersClusterState = Map(
          peerA -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Offline.some, Some(Set(RoundId("r1"))), Some(100L)),
            peerC -> NodeReconciliationData(peerC, DownloadInProgress.some, None, Some(100L))
          ),
          peerB -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, SnapshotCreation.some, None, Some(100L))
          ),
          peerC -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, Ready.some, None, Some(100L)),
          )
        )

        val expected = Map(
          peerA -> List(NodeAligned),
          peerB -> List(NodeAligned),
          peerC -> List(NodeAligned)
        )

        val result = calculateClusterAlignment(peersClusterState)

        result shouldBe expected
      }

      "when nodes that see offline peers in online state are still running a consensus round for these nodes" in {
        val peersClusterState = Map(
          peerA -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, Some(Set(RoundId("r1"))), Some(100L)),
            peerC -> NodeReconciliationData(peerC, DownloadInProgress.some, None, Some(100L))
          ),
          peerB -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Offline.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, SnapshotCreation.some, None, Some(100L))
          ),
          peerC -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Leaving.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, Ready.some, None, Some(100L)),
          )
        )

        val expected = Map(
          peerA -> List(NodeAligned),
          peerB -> List(NodeAligned),
          peerC -> List(NodeAligned)
        )

        val result = calculateClusterAlignment(peersClusterState)

        result shouldBe expected
      }

      "when node doesn't see other node and nodes that see that node indicate that it's still joining" in {
        val peersClusterState = Map(
          peerA -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, DownloadInProgress.some, None, Some(100L))
          ),
          peerB -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, ReadyForDownload.some, None, None),
            peerC -> NodeReconciliationData(peerC, SnapshotCreation.some, None, Some(100L))
          ),
          peerC -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, PendingDownload.some, None, None),
            peerC -> NodeReconciliationData(peerC, Ready.some, None, Some(100L)),
          )
        )

        val expected = Map(
          peerA -> List(NodeAligned),
          peerB -> List(NodeAligned),
          peerC -> List(NodeAligned)
        )

        val result = calculateClusterAlignment(peersClusterState)

        result shouldBe expected
      }

      "when node doesn't see other node and nodes that see that node indicate that it's leaving or offline" in {
        val peersClusterState = Map(
          peerA -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, DownloadInProgress.some, None, Some(100L))
          ),
          peerB -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Offline.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, SnapshotCreation.some, None, Some(100L))
          ),
          peerC -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Leaving.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, Ready.some, None, Some(100L)),
          )
        )

        val expected = Map(
          peerA -> List(NodeAligned),
          peerB -> List(NodeAligned),
          peerC -> List(NodeAligned)
        )

        val result = calculateClusterAlignment(peersClusterState)

        result shouldBe expected
      }

      "when node doesn't see other node and nodes that see that node are still running consensus for that node" in {
        val peersClusterState = Map(
          peerA -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, DownloadInProgress.some, None, Some(100L))
          ),
          peerB -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, None, Some(100L)), // we should ignore the node that the healthcheck is run for in this case's logic
            peerC -> NodeReconciliationData(peerC, SnapshotCreation.some, None, Some(100L))
          ),
          peerC -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, DownloadInProgress.some, Some(Set(RoundId("r1"))), Some(100L)),
            peerC -> NodeReconciliationData(peerC, Ready.some, None, Some(100L)),
          )
        )

        val expected = Map(
          peerA -> List(NodeAligned),
          peerB -> List(NodeAligned),
          peerC -> List(NodeAligned)
        )

        val result = calculateClusterAlignment(peersClusterState)

        result shouldBe expected
      }

      "when node that doesn't see other node is still running consensus round for that node" in {
        val peersClusterState = Map(
          peerA -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            // we should have an info here about the node that we are running consensus for, even if we don't have it in our list, unless we have that -> this case won't work
            peerB -> NodeReconciliationData(peerB, None, Some(Set(RoundId("r1"))), None),
            peerC -> NodeReconciliationData(peerC, DownloadInProgress.some, None, Some(100L))
          ),
          peerB -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, None, None),
            peerC -> NodeReconciliationData(peerC, SnapshotCreation.some, None, Some(100L))
          ),
          peerC -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, DownloadInProgress.some, None, None),
            peerC -> NodeReconciliationData(peerC, Ready.some, None, Some(100L)),
          )
        )

        val expected = Map(
          peerA -> List(NodeAligned),
          peerB -> List(NodeAligned),
          peerC -> List(NodeAligned)
        )

        val result = calculateClusterAlignment(peersClusterState)

        result shouldBe expected
      }
    }

    "should indicate that node is not present on all nodes" - {
      """when node is not present and other nodes don't indicate that it is joining or leaving or the node that has
         that node missing is still running consensus round or the nodes that still see it are running consensus round""" in {
        val peersClusterState = Map(
          peerA -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, DownloadInProgress.some, None, Some(100L))
          ),
          peerB -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, None, None),
            peerC -> NodeReconciliationData(peerC, SnapshotCreation.some, None, Some(100L))
          ),
          peerC -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, DownloadInProgress.some, None, None),
            peerC -> NodeReconciliationData(peerC, Ready.some, None, Some(100L)),
          )
        )

        val expected = Map(
          peerA -> List(NodeAligned),
          peerB -> List(NodeNotPresentOnAllNodes(Set(peerB, peerC), Set(peerA))),
          peerC -> List(NodeAligned)
        )

        val result = calculateClusterAlignment(peersClusterState)

        result shouldBe expected
      }
    }

    "should indicate that node is inconsistently seen as online or offline" - {
      """when node is seen as offline by some nodes and online by others but neither all nodes perceiving node as
         offline nor all nodes perceiving node as online are still running consensus round""" in {
        val peersClusterState = Map(
          peerA -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Offline.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, DownloadInProgress.some, None, Some(100L))
          ),
          peerB -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, Ready.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, SnapshotCreation.some, None, Some(100L))
          ),
          peerC -> Map(
            peerA -> NodeReconciliationData(peerA, Ready.some, None, Some(100L)),
            peerB -> NodeReconciliationData(peerB, DownloadInProgress.some, None, Some(100L)),
            peerC -> NodeReconciliationData(peerC, Ready.some, None, Some(100L)),
          )
        )

        val expected = Map(
          peerA -> List(NodeAligned),
          peerB -> List(NodeInconsistentlySeenAsOnlineOrOffline(Set(peerB, peerC), Set(peerA))),
          peerC -> List(NodeAligned)
        )

        val result = calculateClusterAlignment(peersClusterState)

        result shouldBe expected
      }
    }
  }
}
