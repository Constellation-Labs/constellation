package org.constellation.infrastructure.snapshot

import better.files.File
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import org.constellation.ConstellationExecutionContext
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.schema.checkpoint.CheckpointCache
import org.constellation.serializer.KryoSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedMap

class SnapshotInfoLocalStorageTest extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val timer: Timer[IO] = IO.timer(ConstellationExecutionContext.unbounded)

  "createDirectoryIfNotExists" - {
    "should create snapshot info directory if it does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)

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
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
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
        val snapshotInfoStorage = SnapshotLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotInfoStorage.exists("unknown_file").unsafeRunSync shouldBe false
      }
    }
  }

  "readSnapshotInfo" - {
    "should return SnapshotInfo if snapshot info exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val snapshot = Snapshot("lastHash", Seq.empty[String], SortedMap.empty)
        val storedSnapshot = StoredSnapshot(snapshot, Seq.empty[CheckpointCache])
        val snapshotInfo = SnapshotInfo(storedSnapshot)

        val hash = "abc123"
        val bytes = KryoSerializer.serialize(snapshotInfo)

        (snapshotInfosDir / hash).writeBytes(bytes.toIterator)

        snapshotInfoStorage.read(hash).value.flatMap(IO.fromEither).unsafeRunSync shouldBe snapshotInfo
      }
    }
    "should return an error if snapshot info does not exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotInfoStorage.read("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
    "should return an error if snapshot info file cannot be deserialized to SnapshotInfo" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        (snapshotInfosDir / "known").write("hello world")

        snapshotInfoStorage.read("known").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }

  "writeSnapshotInfo" - {
    "should write snapshot info on disk using the hash as a filename" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val hash = "abc123"
        val bytes = "hello world".getBytes
        snapshotInfoStorage.write(hash, bytes).value.unsafeRunSync

        (snapshotInfosDir / hash).exists shouldBe true
        (snapshotInfosDir / hash).loadBytes shouldBe bytes
      }
    }
  }

  "removeSnapshotInfo" - {
    "should remove snapshot info from disk if exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotInfosDir.some) { file =>
          val name = file.name
          file.write("hello world")
          snapshotInfoStorage.delete(name).value.flatMap(IO.fromEither).unsafeRunSync
          (snapshotInfosDir / name).exists shouldBe false
        }
      }
    }
    "should return error if snapshot info does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotInfoStorage.delete("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }

  "getSnapshotInfoHashes" - {
    "should return iterator with all snapshot filenames (hashes) from snapshot directory" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotInfosDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotInfosDir.some) { file2 =>
            snapshotInfoStorage.list().rethrowT.map(_.toSeq.sorted).unsafeRunSync shouldBe Seq(file1, file2)
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
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotInfosDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotInfosDir.some) { file2 =>
            snapshotInfoStorage.getFiles().rethrowT.map(_.toSeq.sortBy(_.name)).unsafeRunSync shouldBe Seq(
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
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotInfosDir.some) { file2 =>
          file2.write("hello world")
          val bytes = file2.loadBytes
          snapshotInfoStorage.readBytes(file2.name).value.flatMap(IO.fromEither).unsafeRunSync shouldBe bytes
        }
      }
    }
    "should return an error if snapshot does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotInfosDir = dir / "snapshot_infos"
        val snapshotInfoStorage = SnapshotInfoLocalStorage[IO](snapshotInfosDir.pathAsString)
        snapshotInfoStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotInfoStorage.readBytes("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }
}
