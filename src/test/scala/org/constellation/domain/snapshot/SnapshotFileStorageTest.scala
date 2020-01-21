package org.constellation.domain.snapshot

import org.scalatest.{FreeSpec, Matchers}
import better.files._
import java.io.{File => JFile}

import cats.effect.IO
import cats.implicits._
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.infrastructure.snapshot.SnapshotFileStorage
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.serializer.KryoSerializer

class SnapshotFileStorageTest extends FreeSpec with Matchers {
  "createDirectoryIfNotExists" - {
    "should create snapshot directory if it does not exist" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)

        snapshotsDir.exists shouldBe false
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync
        snapshotsDir.exists shouldBe true
      }
    }
  }

  "exists" - {
    "should return true if snapshot exists" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file =>
          snapshotStorage.exists(file.name).unsafeRunSync shouldBe true
        }
      }
    }
    "should return false if snapshot does not exist" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotStorage.exists("unknown_file").unsafeRunSync shouldBe false
      }
    }
  }

  "readSnapshot" - {
    "should return StoredSnapshot if snapshot exists" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val snapshot = Snapshot("lastHash", Seq.empty[String])
        val storedSnapshot = StoredSnapshot(snapshot, Seq.empty[CheckpointCache])

        val hash = "abc123"
        val bytes = KryoSerializer.serialize(storedSnapshot)

        (snapshotsDir / hash).writeBytes(bytes.toIterator)

        snapshotStorage.readSnapshot(hash).value.flatMap(IO.fromEither).unsafeRunSync shouldBe storedSnapshot
      }
    }
    "should return an error if snapshot does not exists" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotStorage.readSnapshot("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
    "should return an error if snapshot file cannot be deserialized to StoredSnapshot" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        (snapshotsDir / "known").write("hello world")

        snapshotStorage.readSnapshot("known").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }

  "writeSnapshot" - {
    "should write snapshot on disk using the hash as a filename" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val hash = "abc123"
        val bytes = "hello world".getBytes
        snapshotStorage.writeSnapshot(hash, bytes).value.unsafeRunSync

        (snapshotsDir / hash).exists shouldBe true
        (snapshotsDir / hash).loadBytes shouldBe bytes
      }
    }
  }

  "removeSnapshot" - {
    "should remove snapshot from disk if exists" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file =>
          val name = file.name
          file.write("hello world")
          snapshotStorage.removeSnapshot(name).value.flatMap(IO.fromEither).unsafeRunSync
          (snapshotsDir / name).exists shouldBe false
        }
      }
    }
    "should return error if snapshot does not exist" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotStorage.removeSnapshot("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }

  "getUsableSpace" - {
    "should read usable space from snapshot directory" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val usableSpace = snapshotsDir.toJava.getUsableSpace

        snapshotStorage.getUsableSpace.unsafeRunSync shouldBe usableSpace
      }
    }
  }

  "getOccupiedSpace" - {
    "should read occupied space from snapshot directory" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val occupiedSpace = snapshotsDir.size

        snapshotStorage.getOccupiedSpace.unsafeRunSync shouldBe occupiedSpace
      }
    }
  }

  "getSnapshotHashes" - {
    "should return iterator with all snapshot filenames (hashes) from snapshot directory" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotsDir.some) { file2 =>
            snapshotStorage.getSnapshotHashes.map(_.toSeq.sorted).unsafeRunSync shouldBe Seq(file1, file2)
              .map(_.name)
              .sorted
          }
        }
      }
    }
  }

  "getSnapshotFiles" - {
    "should return iterator with all snapshot files from snapshot directory" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotsDir.some) { file2 =>
            snapshotStorage.getSnapshotFiles.map(_.toSeq.sortBy(_.name)).unsafeRunSync shouldBe Seq(file1, file2)
              .sortBy(_.name)
          }
        }
      }
    }
  }

  "getSnapshotBytes" - {
    "should read snapshot as bytes if snapshot exists" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file2 =>
          file2.write("hello world")
          val bytes = file2.loadBytes
          snapshotStorage.getSnapshotBytes(file2.name).value.flatMap(IO.fromEither).unsafeRunSync shouldBe bytes
        }
      }
    }
    "should return an error if snapshot does not exist" in {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotFileStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotStorage.getSnapshotBytes("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }
}
