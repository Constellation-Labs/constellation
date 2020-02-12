package org.constellation.util

import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext
import org.constellation.Fixtures.{toRecentSnapshot, toRecentSnapshotWithPrefix}
import org.constellation.domain.redownload.RedownloadService
import org.constellation.schema.Id
import org.constellation.storage.RecentSnapshot
import org.mockito.ArgumentMatchersSugar
import org.scalatest.{FreeSpec, FunSpecLike, Matchers}

class MajorityStateChooserFreeTest extends FreeSpec with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val majorityState = new MajorityStateChooser[IO]

  "compare snapshot state" - {
    "should return empty list to be deleted but not below given height" - {
      val ownSnapshots = List(6, 5, 4, 3).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val majorState = (List(7, 6, 5, 4, 3).map(i => RecentSnapshot(s"$i", i, Map.empty)), Set(Id("node1")))

      val diff = MajorityStateChooser.compareSnapshotState(majorState, ownSnapshots)

      diff.snapshotsToDelete shouldBe List()
      diff.snapshotsToDownload shouldBe List(RecentSnapshot("7", 7, Map.empty))
      diff.peers.size shouldBe 1
    }

    "should return no diff" - {
      val ownSnapshots = List(4, 3, 2, 1).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val majorState = (List(4, 3, 2, 1).map(i => RecentSnapshot(s"$i", i, Map.empty)), Set[Id]())

      val diff = MajorityStateChooser.compareSnapshotState(majorState, ownSnapshots)

      diff.snapshotsToDelete shouldBe List()
      diff.snapshotsToDownload shouldBe List()
    }
  }

  "choose majority state" - {
    "when node creates a correct snapshot" - {
      "still chooses other nodes if majority at given height is equal" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("ownNode") :: Nil
        val node1 = Id("node1") -> List(0, 2, 4, 6).map(toRecentSnapshot)
        val node2 = Id("node2") -> List(0, 2, 4, 6).map(toRecentSnapshot)
        val ownNode = Id("ownNode") -> List(0, 2).map(toRecentSnapshot)
        val nodeList = List(node1, node2, ownNode)
        val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

        sortedSnaps shouldBe List(6, 4, 2, 0).map(toRecentSnapshot)
      }
    }

    //todo missing lower bound, what snap does the node have locally
    "when some majority proposals are missing a lower bound" - {
      "chooses only nodes that have lower bound for redownload" in {

      }
    }
    "when node creates an incorrect snapshot" - {
      "chooses all nodes who has the majority at given height if all of them have same amount of snapshots" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("ownNode") :: Nil
        val node1 = Id("node1") -> List(0, 2, 4, 6, 8).map(toRecentSnapshot)
        val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(toRecentSnapshot)
        val node3 = Id("node3") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6, 8).map(
          toRecentSnapshotWithPrefix("a")
        ))
        val ownNode = Id("ownNode") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6).map(
          toRecentSnapshotWithPrefix("b")
        ))
        val nodeList = List(node1, node2, node3, ownNode)
        val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

        sortedSnaps shouldBe List(8, 6, 4, 2, 0).map(toRecentSnapshot)
        nodeIdsWithSnaps shouldBe Set(Id("node1"), Id("node2"))
      }

      "choose the majority at the majority height when other peers heights are above majority" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("ownNode") :: Nil
        val node1 = Id("node1") -> List(0, 2, 4, 6, 8, 10).map(toRecentSnapshot)
        val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(toRecentSnapshot)
        val node3 = Id("node3") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6, 8).map(
          toRecentSnapshotWithPrefix("a")
        ))

        val ownNode = Id("ownNode") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6).map(
          toRecentSnapshotWithPrefix("b")
        ))
        val nodeList = List(node1, node2, node3, ownNode)
        val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

        sortedSnaps shouldBe List(8, 6, 4, 2, 0).map(toRecentSnapshot)
        nodeIdsWithSnaps shouldBe Set(Id("node1"), Id("node2"))
      }

      "chooses correct majority when encountering non-50% split" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("node4") :: Id("node5") :: Id("ownNode") :: Nil

        val node1 = Id("node1") -> List(0, 2, 4, 6, 8, 10).map(toRecentSnapshot)
        val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(toRecentSnapshot)
        val node3 = Id("node3") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6, 8).map(
          toRecentSnapshotWithPrefix("a")
        ))
        val node4 = Id("node4") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6, 8).map(
          toRecentSnapshotWithPrefix("a")
        ))
        val node5 = Id("node5") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6).map(toRecentSnapshotWithPrefix("b")))

        val ownNode = Id("ownNode") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6).map(
          toRecentSnapshotWithPrefix("b")
        ))

        val nodeList = List(node1, node2, node3, node4, node5, ownNode)
        val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

        sortedSnaps shouldBe List(6, 4, 2, 0).map(toRecentSnapshot)
        nodeIdsWithSnaps shouldBe Set(Id("node1"), Id("node2"))
      }

      "chooses correct majority when encountering a 1/3 split" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Nil

        val node1 = Id("node1") -> (List(0, 2, 4, 6, 8).map(toRecentSnapshot) ++ List(
          toRecentSnapshotWithPrefix("a")(10)
        ))
        val node2 = Id("node2") -> (List(0, 2, 4, 6, 8).map(toRecentSnapshot) ++ List(
          toRecentSnapshotWithPrefix("b")(10)
        ))
        val node3 = Id("node3") -> (List(0, 2, 4, 6, 8).map(toRecentSnapshot) ++ List(
          toRecentSnapshotWithPrefix("c")(10)
        ))

        val nodeList = List(node1, node2, node3)
        val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

        sortedSnaps shouldBe List(toRecentSnapshotWithPrefix("a")(10)) ++ List(8, 6, 4, 2, 0).map(toRecentSnapshot)
        nodeIdsWithSnaps shouldBe Set(Id("node1"))
      }
    }
  }

  "when a node is too" - {
    "high, redownload to majority snapshot" in {
      val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("node4") :: Id("node5") :: Id("ownNode") :: Nil
      val snapsToDelete = List.tabulate(50)(n => 2 * n).takeRight(47).map(toRecentSnapshotWithPrefix("b")).reverse

      val node1 = Id("node1") -> List(0, 2, 4, 6, 8, 10).map(toRecentSnapshot)
      val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(toRecentSnapshot)
      val node3 = Id("node3") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6, 8).map(
        toRecentSnapshotWithPrefix("a")
      ))
      val node4 = Id("node4") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6, 8).map(
        toRecentSnapshotWithPrefix("a")
      ))
      val node5 = Id("node5") -> (List(0, 2, 4).map(toRecentSnapshot) ++ List(6).map(toRecentSnapshotWithPrefix("b")))

      val ownNode = Id("ownNode") -> (List(0, 2, 4).map(toRecentSnapshot) ++ snapsToDelete)

      val nodeList = List(node1, node2, node3, node4, node5, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      val diff = MajorityStateChooser.compareSnapshotState((sortedSnaps, nodeIdsWithSnaps), ownNode._2)
      val willReDownload = RedownloadService.shouldReDownload(ownNode._2, diff)

      sortedSnaps shouldBe List(6, 4, 2, 0).map(toRecentSnapshot)
      nodeIdsWithSnaps shouldBe Set(Id("node1"), Id("node2"))
      diff.snapshotsToDelete shouldBe snapsToDelete
      willReDownload shouldBe true
    }

    "low, redownload to majority snapshot" in {
      val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("node4") :: Id("node5") :: Id("ownNode") :: Nil
      val correctMajResult = List.tabulate(46)(n => 2 * n).map(toRecentSnapshot).reverse

      val node1 = Id("node1") -> List.tabulate(50)(n => 2 * n).map(toRecentSnapshot)
      val node2 = Id("node2") -> List.tabulate(50)(n => 2 * n).map(toRecentSnapshot)
      val node3 = Id("node3") -> (List.tabulate(46)(n => 2 * n).map(toRecentSnapshot) ++ List(92, 94).map(
        toRecentSnapshotWithPrefix("a")
      ))
      val node4 = Id("node4") -> (List.tabulate(46)(n => 2 * n).map(toRecentSnapshot) ++ List(92, 94).map(
        toRecentSnapshotWithPrefix("a")
      ))
      val node5 = Id("node5") -> (List.tabulate(46)(n => 2 * n).map(toRecentSnapshot) ++ List(92).map(
        toRecentSnapshotWithPrefix("b")
      ))

      val ownNode = Id("ownNode") -> (List
        .tabulate(20)(n => 2 * n)
        .map(toRecentSnapshot) ++ List.tabulate(25)(n => 2 * n).takeRight(5).map(toRecentSnapshotWithPrefix("b")))

      val nodeList = List(node1, node2, node3, node4, node5, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      val diff = MajorityStateChooser.compareSnapshotState((sortedSnaps, nodeIdsWithSnaps), ownNode._2)
      val willReDownload = RedownloadService.shouldReDownload(ownNode._2, diff)

      sortedSnaps shouldBe correctMajResult
      nodeIdsWithSnaps shouldBe Set(Id("node1"), Id("node2"), Id("node3"), Id("node4"), Id("node5"))
      diff.snapshotsToDownload shouldBe correctMajResult.take(26)
      willReDownload shouldBe true
    }
  }
}

class MajorityStateChooserTest extends FunSpecLike with ArgumentMatchersSugar with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val majorityState = new MajorityStateChooser[IO]
  val allPeers = Id("node1") :: Id("node2") :: Id("ownNode") :: Nil

  describe("Should return correct major state") {
    it("after receiving the snapshots") {
      val node1 = Id("node1") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val nodeList = List(node1, node2, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshots in reverse order from first node") {
      val node1 = Id("node1") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val nodeList = List(node1, node2, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshots in reverse order from nodes") {
      val node1 = Id("node1") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val nodeList = List(node1, node2, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshot in a mixed order from nodes") {
      val node1 = Id("node1") -> List(2, 0, 6, 4).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(4, 6, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val nodeList = List(node1, node2, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the major state is lower than own snapshots") {
      val node1 = Id("node1") -> List(2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val nodeList = List(node1, node2, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"), Id("ownNode"))) shouldBe true
    }

    it("after receiving the snapshots with a difference greater than 10") {
      val node1 = Id("node1") -> List(14, 12, 10, 8, 6, 4, 2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2, 4).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val nodeList = List(node1, node2, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      sortedSnaps shouldBe List(
        RecentSnapshot("14", 14, Map.empty),
        RecentSnapshot("12", 12, Map.empty),
        RecentSnapshot("10", 10, Map.empty),
        RecentSnapshot("8", 8, Map.empty),
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"))) shouldBe true
    }

    it("after receiving empty list from one node") {
      val node1 = Id("node1") -> List()
      val node2 = Id("node2") -> List(2, 0).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2, 4, 6, 8).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val nodeList = List(node1, node2, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      sortedSnaps shouldBe List(
        RecentSnapshot("8", 8, Map.empty),
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node2"), Id("ownNode"))) shouldBe true
    }

    it("after receiving empty lists from all nodes") {
      val node1 = Id("node1") -> List()
      val node2 = Id("node2") -> List()
      val ownNode = Id("ownNode") -> List()
      val nodeList = List(node1, node2, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers)

      sortedSnaps shouldBe Seq()
      nodeIdsWithSnaps shouldBe Set()
    }

    it("after receiving inconsistent lists from all nodes") {
      val allPeers3 = Id("node1") :: Id("node2") :: Id("node3") :: Id("ownNode") :: Nil

      val node1 = Id("node1") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val node3 = Id("node3") -> List(0, 2, 6).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val ownNode = Id("ownNode") -> List(0, 2).map(i => RecentSnapshot(s"$i", i, Map.empty))
      val nodeList = List(node1, node2, node3, ownNode)
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers3)

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"), Id("node3"))) shouldBe true
    }

    it("after receiving snapshots with different hashes") {
      val allPeers4 = Id("node1") :: Id("node2") :: Id("node3") :: Id("node4") :: Nil
      val nodeList = List(
        (
          Id("node1"),
          List(
            RecentSnapshot("foo", 4, Map.empty),
            RecentSnapshot("bar", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          )
        ),
        (
          Id("node2"),
          List(
            RecentSnapshot("foo", 4, Map.empty),
            RecentSnapshot("bar", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          )
        ),
        (
          Id("node3"),
          List(
            RecentSnapshot("foo", 4, Map.empty),
            RecentSnapshot("bar", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          )
        ),
        (
          Id("node4"),
          List(
            RecentSnapshot("6", 6, Map.empty),
            RecentSnapshot("4", 4, Map.empty),
            RecentSnapshot("biz", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          )
        )
      )
      val (sortedSnaps, nodeIdsWithSnaps) = MajorityStateChooser.reDownloadPlan(nodeList, allPeers4)

      sortedSnaps shouldBe List(
        RecentSnapshot("foo", 4, Map.empty),
        RecentSnapshot("bar", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )
      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"), Id("node3"))) shouldBe true
    }
  }

  private def maxOrZero(list: List[RecentSnapshot]): Long =
    list match {
      case Nil      => 0
      case nonEmpty => nonEmpty.map(_.height).max
    }
}
