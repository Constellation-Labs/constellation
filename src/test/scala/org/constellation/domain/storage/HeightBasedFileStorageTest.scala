package org.constellation.domain.storage

import better.files.File
import cats.effect.{Concurrent, ContextShift, IO}
import org.constellation.ConstellationExecutionContext
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.serializer.KryoSerializer
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FreeSpec, Matchers}

class HeightBasedFileStorageTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar {

  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  "creates base directory" in {
    File.usingTemporaryDirectory() { dir =>
      class SampleImplementation[F[_]: Concurrent]
          extends HeightBasedFileStorage[F, SnapshotInfo](dir.pathAsString, StorageItemKind.GenesisObservation) {}

      val instance = new SampleImplementation[IO]

      dir.delete()

      dir.exists shouldBe false
      instance.createBaseDirectoryIfNotExists().value.unsafeRunSync()
      dir.exists shouldBe true
    }
  }

  "creates directory per height" in {
    File.usingTemporaryDirectory() { dir =>
      class SampleImplementation[F[_]: Concurrent]
          extends HeightBasedFileStorage[F, SnapshotInfo](dir.pathAsString, StorageItemKind.GenesisObservation) {}

      val instance = new SampleImplementation[IO]

      instance.createDirectoryIfNotExists(4).value.unsafeRunSync()
      instance.createDirectoryIfNotExists(5).value.unsafeRunSync()

      val dirs = dir.children.toList

      dirs.contains(dir / "4") shouldBe true
      dirs.contains(dir / "5") shouldBe true
    }
  }

  "writes at height directory" - {
    "from bytes" in {
      File.usingTemporaryDirectory() { dir =>
        class SampleImplementation[F[_]: Concurrent]
            extends HeightBasedFileStorage[F, SnapshotInfo](dir.pathAsString, StorageItemKind.SnapshotInfo) {}

        val instance = new SampleImplementation[IO]
        val payload = SnapshotInfo(null, Seq("abc"), Seq.empty)
        val serialized = KryoSerializer.serialize(payload)
        val height = 4

        (dir / "4").createDirectory()

        instance.write(height, "hash", serialized).value.unsafeRunSync()

        (dir / "4" / "hash-snapshot_info").exists shouldBe true
      }
    }

    "from instance" in {
      File.usingTemporaryDirectory() { dir =>
        class SampleImplementation[F[_]: Concurrent]
            extends HeightBasedFileStorage[F, SnapshotInfo](dir.pathAsString, StorageItemKind.SnapshotInfo) {}

        val instance = new SampleImplementation[IO]
        val payload = SnapshotInfo(null, Seq("abc"), Seq.empty)
        val height = 4

        instance.write(height, "hash", payload).value.unsafeRunSync()

        (dir / "4" / "hash-snapshot_info").exists shouldBe true
      }
    }

    "creates directory for height if not exists before writing file" in {
      File.usingTemporaryDirectory() { dir =>
        class SampleImplementation[F[_]: Concurrent]
            extends HeightBasedFileStorage[F, SnapshotInfo](dir.pathAsString, StorageItemKind.SnapshotInfo) {}

        val instance = new SampleImplementation[IO]
        val payload = SnapshotInfo(null, Seq("abc"), Seq.empty)
        val height = 4

        (dir / "4").exists shouldBe false

        instance.write(height, "hash", payload).value.unsafeRunSync()

        (dir / "4").exists shouldBe true
      }
    }
  }

  "reads from height directory" - {
    "as instance" in {
      File.usingTemporaryDirectory() { dir =>
        class SampleImplementation[F[_]: Concurrent]
            extends HeightBasedFileStorage[F, SnapshotInfo](dir.pathAsString, StorageItemKind.SnapshotInfo) {}

        val instance = new SampleImplementation[IO]
        val payload = SnapshotInfo(null, Seq("abc"), Seq.empty)

        instance.write(4, "hash", payload).value.unsafeRunSync()

        val result = instance.read(4).value.unsafeRunSync()
        result.getOrElse(null) shouldBe payload
      }
    }

    "as bytes" in {
      File.usingTemporaryDirectory() { dir =>
        class SampleImplementation[F[_]: Concurrent]
            extends HeightBasedFileStorage[F, SnapshotInfo](dir.pathAsString, StorageItemKind.SnapshotInfo) {}

        val instance = new SampleImplementation[IO]
        val payload = SnapshotInfo(null, Seq("abc"), Seq.empty)
        val serialized = KryoSerializer.serialize(payload)

        instance.write(4, "hash", payload).value.unsafeRunSync()

        val result = instance.readBytes(4).value.unsafeRunSync()
        result.getOrElse(null) shouldBe serialized
      }
    }
  }

}
