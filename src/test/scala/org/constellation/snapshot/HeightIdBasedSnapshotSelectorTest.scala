package org.constellation.snapshot

import org.constellation.domain.schema.Id
import org.constellation.storage.{RecentSnapshot, SnapshotVerification, VerificationStatus}
import org.constellation.util.SnapshotDiff
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSuite, Matchers}

class HeightIdBasedSnapshotSelectorTest
    extends FunSuite
    with IdiomaticMockito
    with Matchers
    with ArgumentMatchersSugar {

  val thisNode = Id("bbb")
  val snapshotSelector = new HeightIdBasedSnapshotSelector(thisNode, 8)

  test("selectSnapshotFromBroadcastResponses should handle empty inputs") {
    snapshotSelector.selectSnapshotFromBroadcastResponses(List.empty, List.empty) shouldBe None
  }

  test("selectSnapshotFromRecent should handle empty inputs") {
    snapshotSelector.selectSnapshotFromRecent(List.empty, List.empty) shouldBe None
  }

  test("selectSnapshotFromRecent should handle empty own snapshots") {
    val peer1 =
      (Id("peer1"), List(RecentSnapshot("snap10", 10), RecentSnapshot("snap8", 8), RecentSnapshot("snap6", 6)))
    val peer2 =
      (Id("peer2"), List(RecentSnapshot("differentSnap10", 10), RecentSnapshot("snap8", 8), RecentSnapshot("snap6", 6)))

    snapshotSelector.selectSnapshotFromRecent(List(peer1, peer2), List.empty) shouldBe Some(
      DownloadInfo(SnapshotDiff(List.empty, peer2._2, List(peer2._1)), peer2._2)
    )
  }

  test("selectCorrectRecentSnapshotAtGivenHeight should include self in majority calculations") {
    val own = RecentSnapshot("snap10", 10)

    val peer1 =
      (Id("peer1"), List(RecentSnapshot("snap10", 10), RecentSnapshot("snap8", 8), RecentSnapshot("snap6", 6)))
    val peer2 =
      (Id("peer2"), List(RecentSnapshot("differentSnap10", 10), RecentSnapshot("snap8", 8), RecentSnapshot("snap6", 6)))

    snapshotSelector
      .selectCorrectRecentSnapshotAtGivenHeight(own, List(peer1, peer2)) shouldBe (own, List(peer1._1, thisNode))
  }

  test("selectCorrectRecentSnapshotAtGivenHeight should sort by id when having equal clusters") {
    val own = RecentSnapshot("snap10", 10)

    val peer1 =
      (Id("zzz"), List(RecentSnapshot("differentSnap10", 10), RecentSnapshot("snap8", 8), RecentSnapshot("snap6", 6)))

    snapshotSelector.selectCorrectRecentSnapshotAtGivenHeight(own, List(peer1)) shouldBe (peer1._2.head, List(peer1._1))
  }
  test("selectCorrectRecentSnapshotAtGivenHeight should stick to given height only") {
    val own = RecentSnapshot("snap10", 10)

    val peer1 =
      (Id("zzz"), List(RecentSnapshot("zzz10", 10), RecentSnapshot("snap8", 8), RecentSnapshot("snap6", 6)))
    val peer2 =
      (
        Id("aaa"),
        List(
          RecentSnapshot("differentSnap12", 12),
          RecentSnapshot("aaa10", 10),
          RecentSnapshot("snap8", 8),
          RecentSnapshot("snap6", 6)
        )
      )

    snapshotSelector.selectCorrectRecentSnapshotAtGivenHeight(own, List(peer1, peer2)) shouldBe (peer1._2.head, List(
      peer1._1
    ))
  }
  test("selectMostRecentCorrectSnapshot should select based on height") {

    val peer1 =
      (Id("zzz"), List(RecentSnapshot("zzz10", 10), RecentSnapshot("snap8", 8), RecentSnapshot("snap6", 6)))
    val peer2 =
      (
        Id("aaa"),
        List(
          RecentSnapshot("differentSnap12", 12),
          RecentSnapshot("aaa10", 10),
          RecentSnapshot("snap8", 8),
          RecentSnapshot("snap6", 6)
        )
      )

    snapshotSelector.selectMostRecentCorrectSnapshot(List(peer1, peer2)) shouldBe (peer2._2, List(
      peer2._1
    ))
  }
  test("selectMostRecentCorrectSnapshot should select based on id") {

    val peer1 =
      (
        Id("zzz"),
        List(
          RecentSnapshot("zzz12", 12),
          RecentSnapshot("zzz10", 10),
          RecentSnapshot("snap8", 8),
          RecentSnapshot("snap6", 6)
        )
      )
    val peer2 =
      (
        Id("aaa"),
        List(
          RecentSnapshot("aaa12", 12),
          RecentSnapshot("aaa10", 10),
          RecentSnapshot("snap8", 8),
          RecentSnapshot("snap6", 6)
        )
      )

    snapshotSelector.selectMostRecentCorrectSnapshot(List(peer1, peer2)) shouldBe (peer1._2, List(
      peer1._1
    ))
  }
  test("selectSnapshotFromBroadcastResponses should return None for empty list") {
    val own = List(RecentSnapshot("snap10", 10))
    snapshotSelector.selectSnapshotFromBroadcastResponses(List(), own) shouldBe None
  }

  test("selectSnapshotFromBroadcastResponses should return None when correct clusterState are higher than invalid.") {
    val own = List(RecentSnapshot("snap10", 10))
    val responses = List(Some(SnapshotVerification(Id("aaa"), VerificationStatus.SnapshotCorrect, List.empty)))
    snapshotSelector.selectSnapshotFromBroadcastResponses(responses, own) shouldBe None
  }

  test(
    "selectSnapshotFromBroadcastResponses should return None when invalid equals correct but at given height same RecentSnapshot is being return due to id sorting election."
  ) {
    val own = List(RecentSnapshot("snap10", 10))
    val responses =
      List(Some(SnapshotVerification(Id("aaa"), VerificationStatus.SnapshotInvalid, List(RecentSnapshot("aaa10", 10)))))
    snapshotSelector.selectSnapshotFromBroadcastResponses(responses, own) shouldBe None
  }

  test(
    "selectSnapshotFromBroadcastResponses should return Some when invalid equals correct and at given height different RecentSnapshot is being return due to id sorting election."
  ) {
    val own = List(RecentSnapshot("snap10", 10))
    val responses =
      List(Some(SnapshotVerification(Id("ccc"), VerificationStatus.SnapshotInvalid, List(RecentSnapshot("aaa10", 10)))))
    snapshotSelector.selectSnapshotFromBroadcastResponses(responses, own) shouldBe Some(
      DownloadInfo(
        SnapshotDiff(own, responses.head.get.recentSnapshot, List(responses.head.get.id)),
        responses.head.get.recentSnapshot
      )
    )
  }

  test(
    "selectSnapshotFromBroadcastResponses should return Some when invalid is greater than correct."
  ) {
    val own = List(RecentSnapshot("snap10", 10))
    val responses =
      List(
        Some(SnapshotVerification(Id("ccc"), VerificationStatus.SnapshotInvalid, List(RecentSnapshot("aaa10", 10)))),
        Some(SnapshotVerification(Id("ddd"), VerificationStatus.SnapshotInvalid, List(RecentSnapshot("aaa10", 10))))
      )
    snapshotSelector.selectSnapshotFromBroadcastResponses(responses, own) shouldBe Some(
      DownloadInfo(
        SnapshotDiff(own, responses.head.get.recentSnapshot, List(Id("ccc"), Id("ddd"))),
        responses.head.get.recentSnapshot
      )
    )
  }

}
