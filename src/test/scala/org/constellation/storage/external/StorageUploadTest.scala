package org.constellation.storage.external

import better.files.File
import org.constellation.domain.cloud.CloudStorage.StorageName
import org.constellation.domain.configuration.NodeConfig
import org.constellation.{DAO, Fixtures, ProcessingConfig, TestHelpers}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class StorageUploadTest extends FunSuite with Matchers with BeforeAndAfter {

  implicit var dao: DAO = _

  before {
    dao = TestHelpers.prepareRealDao(
      nodeConfig =
        NodeConfig(primaryKeyPair = Fixtures.tempKey5, processingConfig = ProcessingConfig(metricCheckInterval = 200))
    )
  }

  after {
    dao.unsafeShutdown()
  }

  ignore("should send snapshots") {
    val snapshots: Seq[File] = File("src/test/resources/rollback_data/snapshots").children.toSeq

    val savedSnapshots = dao.cloudStorage.upload(snapshots, StorageName.Snapshot).unsafeRunSync()

    snapshots.size shouldBe savedSnapshots.size
  }
}
