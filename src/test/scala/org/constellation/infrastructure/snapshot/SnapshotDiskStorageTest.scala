package org.constellation.infrastructure.snapshot

import better.files._
import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.ConstellationExecutionContext
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.serializer.KryoSerializer
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}

import scala.collection.SortedMap
import scala.concurrent.ExecutionContextExecutor

object CheckOpenedFileDescriptors {

  def check(f: => Any): Unit = {
    val startOpenFiles = File.numberOfOpenFileDescriptors()
    f
    val endOpenFiles = File.numberOfOpenFileDescriptors()
    println(s"${Console.RED}${endOpenFiles} - ${startOpenFiles} = ${endOpenFiles - startOpenFiles}${Console.RESET}")
  }
}

class SnapshotDiskStorageTest extends FreeSpec with Matchers with BeforeAndAfterAll {

  implicit val ec: ExecutionContextExecutor = ConstellationExecutionContext.unbounded
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)

  var openedFiles: Long = 0

  override def beforeAll(): Unit =
    openedFiles = File.numberOfOpenFileDescriptors()

  override def afterAll(): Unit = {
    val endOpenFiles = File.numberOfOpenFileDescriptors()
    println(s"${Console.YELLOW}${endOpenFiles} - ${openedFiles} = ${endOpenFiles - openedFiles}${Console.RESET}")
  }

  "createDirectoryIfNotExists" - {
    "should create snapshot directory if it does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)

        snapshotsDir.exists shouldBe false
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync
        snapshotsDir.exists shouldBe true
      }
    }
  }

  "exists" - {
    "should return true if snapshot exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotsDir.some) { file2 =>
            snapshotStorage.exists(file1.name).unsafeRunSync shouldBe true
            snapshotStorage.exists(file2.name).unsafeRunSync shouldBe true
          }
        }
      }
    }
    "should return false if snapshot does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotStorage.exists("unknown_file").unsafeRunSync shouldBe false
      }
    }
  }

  "readSnapshot" - {
    "should return StoredSnapshot if snapshot exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val snapshot = Snapshot("lastHash", Seq.empty[String], SortedMap.empty)
        val storedSnapshot = StoredSnapshot(snapshot, Seq.empty[CheckpointCache])

        val hash = "abc123"
        val bytes = KryoSerializer.serialize(storedSnapshot)

        (snapshotsDir / hash).writeBytes(bytes.toIterator)

        snapshotStorage.read(hash).rethrowT.unsafeRunSync shouldBe storedSnapshot
      }
    }
    "should return an error if snapshot does not exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotStorage.read("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
    "should return an error if snapshot file cannot be deserialized to StoredSnapshot" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        (snapshotsDir / "known").write("hello world")

        snapshotStorage.read("known").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }

  "writeSnapshot" - {
    "should write snapshot on disk using the hash as a filename" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val hash = "abc123"
        val bytes = "hello world".getBytes
        snapshotStorage.write(hash, bytes).value.unsafeRunSync

        (snapshotsDir / hash).exists shouldBe true
        (snapshotsDir / hash).loadBytes shouldBe bytes
      }
    }
  }

  "removeSnapshot" - {
    "should remove snapshot from disk if exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file =>
          val name = file.name
          file.write("hello world")
          snapshotStorage.delete(name).value.flatMap(IO.fromEither).unsafeRunSync
          (snapshotsDir / name).exists shouldBe false
        }
      }
    }
    "should return error if snapshot does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotStorage.delete("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }

  "getUsableSpace" - {
    "should read usable space from snapshot directory" ignore CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val usableSpace = snapshotsDir.toJava.getUsableSpace

        snapshotStorage.getUsableSpace.unsafeRunSync shouldBe usableSpace
      }
    }
  }

  "getOccupiedSpace" - {
    "should read occupied space from snapshot directory" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        val occupiedSpace = snapshotsDir.size

        snapshotStorage.getOccupiedSpace.unsafeRunSync shouldBe occupiedSpace
      }
    }
  }

  "getSnapshotHashes" - {
    "should return iterator with all snapshot filenames (hashes) from snapshot directory" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotsDir.some) { file2 =>
            snapshotStorage.list().rethrowT.map(_.toSeq.sorted).unsafeRunSync shouldBe Seq(file1, file2)
              .map(_.name)
              .sorted
          }
        }
      }
    }
  }

  "getSnapshotFiles" - {
    "should return iterator with all snapshot files from snapshot directory" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file1 =>
          File.usingTemporaryFile("", "", snapshotsDir.some) { file2 =>
            snapshotStorage.getFiles().rethrowT.map(_.toSeq.sortBy(_.name)).unsafeRunSync shouldBe Seq(file1, file2)
              .sortBy(_.name)
          }
        }
      }
    }
  }

  "getSnapshotBytes" - {
    "should read snapshot as bytes if snapshot exists" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        File.usingTemporaryFile("", "", snapshotsDir.some) { file2 =>
          file2.write("hello world")
          val bytes = file2.loadBytes
          snapshotStorage.readBytes(file2.name).value.flatMap(IO.fromEither).unsafeRunSync shouldBe bytes
        }
      }
    }
    "should return an error if snapshot does not exist" in CheckOpenedFileDescriptors.check {
      File.usingTemporaryDirectory() { dir =>
        val snapshotsDir = dir / "snapshots"
        val snapshotStorage = SnapshotLocalStorage[IO](snapshotsDir.pathAsString)
        snapshotStorage.createDirectoryIfNotExists().value.unsafeRunSync

        snapshotStorage.readBytes("unknown").value.map(_.isLeft).unsafeRunSync shouldBe true
      }
    }
  }
}
