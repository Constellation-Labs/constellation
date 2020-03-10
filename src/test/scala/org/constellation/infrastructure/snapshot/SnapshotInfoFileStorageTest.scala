package org.constellation.infrastructure.snapshot

import better.files.File
import cats.effect.IO
import cats.implicits._
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.serializer.KryoSerializer
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}

import scala.collection.SortedMap

class SnapshotInfoFileStorageTest extends FreeSpec with Matchers with BeforeAndAfterAll {

  "createDirectoryIfNotExists" - {
    "should create snapshot info directory if it does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)

        snapshotInfosDir.exists shouldBe false
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync
        snapshotInfosDir.exists shouldBe true
      }
    }
  }

  "exists" - {
    "should return true if snapshot info exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotInfosDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotInfosDir.some) { file2 =>
            snapshotInfoStorage.exists(file1.name).unsafeRunSync shouldBe true
            snapshotInfoStorage.exists(file2.name).unsafeRunSync shouldBe true
          }
        }
      }
    }
    "should return false if snapshot info does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshots"
        val snapshotInfoStorage = SnapshotDiskFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotInfoStorage.exists("unknown_file").unsafeRunSync shouldBe false
      }
    }
  }

  "readSnapshotInfo" - {
    "should return SnapshotInfo if snapshot info exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val snapshot = Snapshot("lastHash", Seq.empty[String], SortedMap.empty)
        val storedSnapshot = StoredSnapshot(snapshot, Seq.empty[CheckpointCache])
        val snapshotInfo = SnapshotInfo(storedSnapshot)

        val hash = "abc123"
        val bytes = KryoSerializer.serialize(snapshotInfo)

        (snapshotInfosDir / hash).writeBytes(bytes.toIterator)

        snapshotInfoStorage.readSnapshotInfo(hash).value.flatMap(IO.fromEither).unsafeRunSync shouldBe snapshotInfo
      }
    }
    "should return an error if snapshot info does not exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotInfoStorage.readSnapshotInfo("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
    "should return an error if snapshot info file cannot be deserialized to SnapshotInfo" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        (snapshotInfosDir / "known").write("hello world")

        snapshotInfoStorage.readSnapshotInfo("known").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }

  "writeSnapshotInfo" - {
    "should write snapshot info on disk using the hash as a filename" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val hash = "abc123"
        val bytes = "hello world".getBytes
        snapshotInfoStorage.writeSnapshotInfo(hash, bytes).value.unsafeRunSync

        (snapshotInfosDir / hash).exists shouldBe true
        (snapshotInfosDir / hash).loadBytes shouldBe bytes
      }
    }
  }

  "removeSnapshotInfo" - {
    "should remove snapshot info from disk if exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotInfosDir.some) { file =>
          val name = file.name
          file.write("hello world")
          snapshotInfoStorage.removeSnapshotInfo(name).value.flatMap(IO.fromEither).unsafeRunSync
          (snapshotInfosDir / name).exists shouldBe false
        }
      }
    }
    "should return error if snapshot info does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotInfoStorage.removeSnapshotInfo("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }

  "getSnapshotInfoHashes" - {
    "should return iterator with all snapshot filenames (hashes) from snapshot directory" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotInfosDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotInfosDir.some) { file2 =>
            snapshotInfoStorage.getSnapshotInfoHashes.map(_.toSeq.sorted).unsafeRunSync shouldBe Seq(file1, file2)
              .map(_.name)
              .sorted
          }
        }
      }
    }
  }

  "getSnapshotInfoFiles" - {
    "should return iterator with all snapshot files from snapshot directory" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotInfosDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotInfosDir.some) { file2 =>
            snapshotInfoStorage.getSnapshotInfoFiles.map(_.toSeq.sortBy(_.name)).unsafeRunSync shouldBe Seq(
              file1,
              file2
            ).sortBy(_.name)
          }
        }
      }
    }
  }

  "getSnapshotInfoBytes" - {
    "should read snapshot as bytes if snapshot exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotInfosDir.some) { file2 =>
          file2.write("hello world")
          val bytes = file2.loadBytes
          snapshotInfoStorage.getSnapshotInfoBytes(file2.name).value.flatMap(IO.fromEither).unsafeRunSync shouldBe bytes
        }
      }
    }
    "should return an error if snapshot does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoFileStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotInfoStorage.getSnapshotInfoBytes("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }
}
