package org.constellation.storage.external

import better.files.File
import org.constellation.domain.configuration.NodeConfig
import org.constellation.util.Metrics
import org.constellation.{DAO, Fixtures}
import org.scalatest.{FunSuite, Matchers}

class StorageUploadTest extends FunSuite with Matchers {

  implicit val dao: DAO = new DAO

  dao.initialize(NodeConfig(primaryKeyPair = Fixtures.tempKey5))
  dao.metrics = new Metrics(200)

  ignore("should send snapshots") {
    val snapshots: Seq[File] = File("src/test/resources/rollback_data/snapshots").children.toSeq

    val savedSnapshots = dao.cloudStorage.upload(snapshots).unsafeRunSync()

    snapshots.size shouldBe savedSnapshots.size
  }
}
