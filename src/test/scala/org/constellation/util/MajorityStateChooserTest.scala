package org.constellation.util

import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext
import org.constellation.Fixtures.{toRecentSnapshot, toRecentSnapshotWithPrefix}
import org.constellation.domain.redownload.{ReDownloadPlan, RedownloadService}
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
        val node1 = Id("node1") -> List(0, 2, 4, 6).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val node2 = Id("node2") -> List(0, 2, 4, 6).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val ownNode = Id("ownNode") -> List(0, 2).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val nodeList = List(node1, node2, ownNode)
        val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) =
          MajorityStateChooser.planReDownload(nodeList.toMap,allPeers, Id("ownNode"))
        //@ReDownloadPlan(id, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals)
        sortedSnaps shouldBe List(6, 4, 2, 0).map(toRecentSnapshot)
      }
    }

    //Note this has been observed and should be prevented via configuration, ensuring enough snapInfos remain on disk
    "when some majority proposals are missing a lower bound" - {
      "chooses only nodes that have lower bound for redownload" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("ownNode") :: Nil
        val node1: (Id, Map[Long, RecentSnapshot]) = Id("node1") -> List(2, 4, 6, 8, 10, 12, 14, 16).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val node2: (Id, Map[Long, RecentSnapshot]) = Id("node2") -> List(6, 8, 10, 12, 14, 16).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val node3: (Id, Map[Long, RecentSnapshot]) = Id("node3") -> (List(2, 4, 6, 8, 10, 12, 14, 16).map(height => (height.toLong -> toRecentSnapshot(height))) ).toMap
        val ownNode: (Id, Map[Long, RecentSnapshot]) = Id("ownNode") -> (List(0, 2).map(height => (height.toLong -> toRecentSnapshot(height)))).toMap
        val allProposals: Map[Id, Map[Long, RecentSnapshot]] = List(node1, node2, node3, ownNode).toMap
        val plan@ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) =
          MajorityStateChooser.planReDownload(allProposals, allPeers, Id("ownNode"))
        sortedSnaps shouldBe Seq(16, 14, 12, 10, 8, 6, 4, 2).map(toRecentSnapshot)
        plan.validRedownloadPeers shouldBe Set(Id("node1"), Id("node3"))
      }
    }

    "when node creates an incorrect snapshot" - {
      "chooses all nodes who has the majority at given height if all of them have same amount of snapshots" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("ownNode") :: Nil
        val node1 = Id("node1") -> List(0, 2, 4, 6, 8).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val node3 = Id("node3") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6, 8).map( height =>
          (height.toLong, (toRecentSnapshotWithPrefix("a")(height)))
        )).toMap
        val ownNode = Id("ownNode") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6).map( height =>
          (height.toLong, (toRecentSnapshotWithPrefix("b")(height)))
        )).toMap
        val allProposals: Map[Id, Map[Long, RecentSnapshot]] = List(node1, node2, node3, ownNode).toMap
        val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(allProposals, allPeers, Id("ownNode"))

        sortedSnaps shouldBe List(8, 6, 4, 2, 0).map(toRecentSnapshot)
        nodeIdsWithSnaps shouldBe Set(Id("node1"), Id("node2"))
      }

      "choose the majority at the majority height when other peers heights are above majority" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("ownNode") :: Nil
        val node1 = Id("node1") -> List(0, 2, 4, 6, 8, 10).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val node3 = Id("node3") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6, 8).map( height =>
          (height.toLong, (toRecentSnapshotWithPrefix("a")(height)))
        )).toMap

        val ownNode = Id("ownNode") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6).map(
          height => (height.toLong, (toRecentSnapshotWithPrefix("b")(height)))
        )).toMap
        val allProposals: Map[Id, Map[Long, RecentSnapshot]] = List(node1, node2, node3, ownNode).toMap
        val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(allProposals, allPeers, Id("ownNode"))

        sortedSnaps shouldBe List(8, 6, 4, 2, 0).map(toRecentSnapshot)
        nodeIdsWithSnaps shouldBe Set(Id("node1"), Id("node2"))
      }

      "chooses correct majority when encountering non-50% split" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("node4") :: Id("node5") :: Id("ownNode") :: Nil

        val node1 = Id("node1") -> List(0, 2, 4, 6, 8, 10).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
        val node3 = Id("node3") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6, 8).map(height =>
            (height.toLong, (toRecentSnapshotWithPrefix("a")(height)))
        )).toMap
        val node4 = Id("node4") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6, 8).map(height =>
            (height.toLong, (toRecentSnapshotWithPrefix("a")(height)))
        )).toMap
        val node5 = Id("node5") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6).map(height =>
          (height.toLong, (toRecentSnapshotWithPrefix("b")(height)))
        )).toMap

        val ownNode = Id("ownNode") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6).map(height =>
          (height.toLong, (toRecentSnapshotWithPrefix("b")(height)))
        )).toMap

        val allProposals = List(node1, node2, node3, node4, node5, ownNode).toMap
        val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(allProposals, allPeers, Id("ownNode"))

        sortedSnaps shouldBe List(6, 4, 2, 0).map(toRecentSnapshot)
        nodeIdsWithSnaps shouldBe Set(Id("node1"), Id("node2"))
      }

      "chooses correct majority when encountering a 1/3 split" in {
        val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Nil

        val node1 = Id("node1") -> (List(0, 2, 4, 6, 8).map(height => (height.toLong -> toRecentSnapshot(height))).toMap ++ List(
          10L -> toRecentSnapshotWithPrefix("a")(10)
        ))
        val node2 = Id("node2") -> (List(0, 2, 4, 6, 8).map(height => (height.toLong -> toRecentSnapshot(height))).toMap ++ List(
          10L -> toRecentSnapshotWithPrefix("b")(10)
        ))
        val node3 = Id("node3") -> (List(0, 2, 4, 6, 8).map(height => (height.toLong -> toRecentSnapshot(height))).toMap ++ List(
          10L -> toRecentSnapshotWithPrefix("c")(10)
        ))

        val allProposals = List(node1, node2, node3).toMap
        val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(allProposals, allPeers, Id("node3"))

        sortedSnaps shouldBe List(toRecentSnapshotWithPrefix("a")(10)) ++ List(8, 6, 4, 2, 0).map(toRecentSnapshot)
        nodeIdsWithSnaps shouldBe Set(Id("node1"))
      }
    }
  }

  "when a node is too" - {
    "high, redownload to majority snapshot" in {
      val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("node4") :: Id("node5") :: Id("ownNode") :: Nil
      val snapsToDelete = List.tabulate(50)(n => 2 * n).takeRight(47).map(height =>
        (height.toLong, (toRecentSnapshotWithPrefix("b")(height)))).reverse

      val node1 = Id("node1") -> List(0, 2, 4, 6, 8, 10).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
      val node2 = Id("node2") -> List(0, 2, 4, 6, 8).map(height => (height.toLong -> toRecentSnapshot(height))).toMap
      val node3 = Id("node3") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6, 8).map(height =>
        (height.toLong, (toRecentSnapshotWithPrefix("a")(height)))
      )).toMap
      val node4 = Id("node4") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6, 8).map(height =>
        (height.toLong, (toRecentSnapshotWithPrefix("a")(height)))
      )).toMap
      val node5 = Id("node5") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ List(6).map(height =>
        (height.toLong, (toRecentSnapshotWithPrefix("b")(height))))).toMap

      val ownNode = Id("ownNode") -> (List(0, 2, 4).map(height => (height.toLong -> toRecentSnapshot(height))) ++ snapsToDelete).toMap

      val allProposals = List(node1, node2, node3, node4, node5, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(allProposals, allPeers, Id("ownNode") )
      val willReDownload = RedownloadService.shouldReDownload(ownNode._2.values.toList, diff, allPeers.toSet)

      sortedSnaps shouldBe List(6, 4, 2, 0).map(toRecentSnapshot)
      nodeIdsWithSnaps shouldBe Set(Id("node1"), Id("node2"))
      diff.snapshotsToDelete shouldBe snapsToDelete.map(_._2)
      willReDownload shouldBe true
    }

    "low, redownload to majority snapshot" in {
      val allPeers = Id("node1") :: Id("node2") :: Id("node3") :: Id("node4") :: Id("node5") :: Id("ownNode") :: Nil
      val correctMajResult = List.tabulate(46)(n => 2 * n).map(toRecentSnapshot).reverse

      val node1 = Id("node1") -> List.tabulate(50)(n => 2 * n).map(height => (height.toLong, toRecentSnapshot(height))).toMap
      val node2 = Id("node2") -> List.tabulate(50)(n => 2 * n).map(height => (height.toLong, toRecentSnapshot(height))).toMap
      val node3 = Id("node3") -> (List.tabulate(46)(n => 2 * n).map(height => (height.toLong, toRecentSnapshot(height))) ++ List(92, 94).map(height => (height.toLong, (toRecentSnapshotWithPrefix("a")(height))))).toMap

      val node4  = Id("node4") -> (List.tabulate(46)(n => 2 * n).map(height => (height.toLong, toRecentSnapshot(height))) ++ List(92, 94).map(height => (height.toLong, (toRecentSnapshotWithPrefix("a")(height))))).toMap
      val node5 = Id("node5") -> (List.tabulate(46)(n => 2 * n).map(height => (height.toLong, toRecentSnapshot(height))) ++ List(92).map(height =>
        (height.toLong, (toRecentSnapshotWithPrefix("b")(height))))).toMap

      val ownNode = Id("ownNode") -> (List
        .tabulate(20)(n => 2 * n)
        .map(height => (height.toLong -> toRecentSnapshot(height))) ++ List.tabulate(25)(n => 2 * n).takeRight(5).map(height =>
        (height.toLong, (toRecentSnapshotWithPrefix("b")(height))))).toMap

      val allProposals: Map[Id, Map[Long, RecentSnapshot]] = List(node1, node2, node3, node4, node5, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(allProposals, allPeers, Id("ownNode"))
      val willReDownload = RedownloadService.shouldReDownload(ownNode._2.values.toList, diff, allPeers.toSet)

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
      val node1 = Id("node1") -> List(0, 2, 4, 6).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val ownNode = Id("ownNode") -> List(0, 2).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val nodeList = List(node1, node2, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers, Id("ownNode"))

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshots in reverse order from first node") {
      val node1 = Id("node1") -> List(6, 4, 2, 0).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val ownNode = Id("ownNode") -> List(0, 2).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val nodeList = List(node1, node2, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers, Id("ownNode"))

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshots in reverse order from nodes") {
      val node1 = Id("node1") -> List(6, 4, 2, 0).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node2 = Id("node2") -> List(6, 4, 2, 0).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val ownNode = Id("ownNode") -> List(0, 2).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val nodeList = List(node1, node2, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers, Id("ownNode"))

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the snapshot in a mixed order from nodes") {
      val node1 = Id("node1") -> List(2, 0, 6, 4).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node2 = Id("node2") -> List(4, 6, 2, 0).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val ownNode = Id("ownNode") -> List(0, 2).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val nodeList = List(node1, node2, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers, Id("ownNode"))

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"))) shouldBe true
    }

    it("after receiving the major state is lower than own snapshots") {
      val node1 = Id("node1") -> List(2, 0).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node2 = Id("node2") -> List(2, 0).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val ownNode = Id("ownNode") -> List(0, 2, 4, 6).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val nodeList = List(node1, node2, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers, Id("ownNode"))

      sortedSnaps shouldBe List(
        RecentSnapshot("6", 6, Map.empty),
        RecentSnapshot("4", 4, Map.empty),
        RecentSnapshot("2", 2, Map.empty),
        RecentSnapshot("0", 0, Map.empty)
      )

      nodeIdsWithSnaps.subsetOf(Set(Id("node1"), Id("node2"), Id("ownNode"))) shouldBe true
    }

    it("after receiving the snapshots with a difference greater than 10") {
      val node1 = Id("node1") -> List(14, 12, 10, 8, 6, 4, 2, 0).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node2 = Id("node2") -> List(2, 0).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val ownNode = Id("ownNode") -> List(0, 2, 4).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val nodeList = List(node1, node2, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers, Id("ownNode"))

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
      val node1 = Id("node1") -> List.empty[Int].map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node2 = Id("node2") -> List(2, 0).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val ownNode = Id("ownNode") -> List(0, 2, 4, 6, 8).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val nodeList = List(node1, node2, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers, Id("ownNode"))

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
      val node1 = Id("node1") -> List.empty[Int].map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node2 = Id("node2") -> List.empty[Int].map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val ownNode = Id("ownNode") -> List.empty[Int].map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val nodeList = List(node1, node2, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers, Id("ownNode"))

      sortedSnaps shouldBe Seq()
      nodeIdsWithSnaps shouldBe Set()
    }

    it("after receiving inconsistent lists from all nodes") {
      val allPeers3 = Id("node1") :: Id("node2") :: Id("node3") :: Id("ownNode") :: Nil

      val node1 = Id("node1") -> List(0, 2, 4, 6).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node2 = Id("node2") -> List(0, 2, 4, 6).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val node3 = Id("node3") -> List(0, 2, 6).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val ownNode = Id("ownNode") -> List(0, 2).map(i => (i.toLong, RecentSnapshot(s"$i", i, Map.empty))).toMap
      val nodeList = List(node1, node2, node3, ownNode).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers3, Id("ownNode"))

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
          ).map(i => (i.height, i)).toMap
        ),
        (
          Id("node2"),
          List(
            RecentSnapshot("foo", 4, Map.empty),
            RecentSnapshot("bar", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          ).map(i => (i.height, i)).toMap
        ),
        (
          Id("node3"),
          List(
            RecentSnapshot("foo", 4, Map.empty),
            RecentSnapshot("bar", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          ).map(i => (i.height, i)).toMap
        ),
        (
          Id("node4"),
          List(
            RecentSnapshot("6", 6, Map.empty),
            RecentSnapshot("4", 4, Map.empty),
            RecentSnapshot("biz", 2, Map.empty),
            RecentSnapshot("0", 0, Map.empty)
          ).map(i => (i.height, i)).toMap
        )
      ).toMap
      val ReDownloadPlan(_, sortedSnaps, nodeIdsWithSnaps, diff, groupedProposals) = MajorityStateChooser.planReDownload(nodeList, allPeers4, Id("node4"))

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
