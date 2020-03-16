package org.constellation.rollback

import org.mockito.ArgumentMatchersSugar
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class RollbackLoaderTest extends FreeSpec with ArgumentMatchersSugar with BeforeAndAfter with Matchers {

  private val existingFolder: String = getClass.getResource("/rollback_data/").getPath
  private val notExistingFolder: String = getClass.getResource("/rollback_data/").getPath + "foobar/"

  private val loaderForExistingFolder = new RollbackLoader(
    existingFolder + "snapshots",
    existingFolder + "snapshot_info",
    existingFolder + "rollback_genesis"
  )
  private val loaderForNotExistingFolder = new RollbackLoader(
    notExistingFolder + "snapshots",
    notExistingFolder + "snapshot_info",
    notExistingFolder + "rollback_genesis"
  )

  "Snapshots Loader" - {
    "should load snapshots if those exists" ignore {
      val snapshots = loaderForExistingFolder.loadSnapshotsFromFile()

      snapshots.isRight shouldBe true
    }
    "should return error if snapshots do not exist" in {
      val snapshots = loaderForNotExistingFolder.loadSnapshotsFromFile()

      snapshots.isLeft shouldBe true
      snapshots.left.get shouldBe a[CannotLoadSnapshotsFiles]
    }
  }

  "GenesisObservation Loader" - {
    "should load genesis observation if this exists" ignore {
      val genesisObservation = loaderForExistingFolder.loadGenesisObservation()

      genesisObservation.isRight shouldBe true
    }
    "should return error if genesis observation doesn't exist" in {
      val genesisObservation = loaderForNotExistingFolder.loadGenesisObservation()

      genesisObservation.isLeft shouldBe true
      genesisObservation.left.get shouldBe a[CannotLoadGenesisObservationFile]
    }
  }

  "SnapshotInfo Loader" - {
    "should load snapshot info if this exists" ignore {
      val snapshotInfo = loaderForExistingFolder.loadSnapshotInfoFromFile()

      snapshotInfo.isRight shouldBe true
    }
    "should return error if snapshot info doesn't exist" in {
      val snapshotInfo = loaderForNotExistingFolder.loadSnapshotInfoFromFile()

      snapshotInfo.isLeft shouldBe true
      snapshotInfo.left.get shouldBe a[CannotLoadSnapshotInfoFile]
    }
  }
}
