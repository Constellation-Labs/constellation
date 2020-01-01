package org.constellation.util

import better.files.File
import cats.effect.{ContextShift, IO}
import org.constellation.Fixtures.kp
import org.constellation._
import org.constellation.consensus._
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.primitives.Schema._
import org.constellation.domain.observation.Observation
import constellation._
import org.constellation.primitives.{ChannelMessage, CheckpointBlock, Schema, Transaction}
import org.constellation.rollback.RollbackLoader
import org.constellation.serializer.KryoSerializer
import org.scalatest.FlatSpec
import Fixtures._

import scala.util.{Random, Try}

class KryoSerializerTest extends FlatSpec {
  val smallInfoParts = "/Users/wyatt/constellation/src/test/resources/snapshot_info_parts"
  val InfoPartsDeflateSerializer = "/Users/wyatt/constellation/src/test/resources/snapshot_info_parts-DeflateSerializer"
  val lrgInfoParts = "/Users/wyatt/constellation/src/test/resources/snapshot_info_parts-lrg"
  val infoSerParts = "/Users/wyatt/constellation/src/test/resources/snapshot_info_parts-ser"

  val random = Random
//  val snapshotInfoLoader = new RollbackLoader(
//    "snapshots",
//    resourceDir + "snapshot_info",
//    "rollback_genesis"
//  )
  def randomAddress = kp.address + random.nextInt(1000).toString

  def generateSnapshotInfo(numCheckpointBlocks: Int = 1000,
                           numAcceptedCBSinceSnapshotCache: Int = 1000,
                           addressCacheDataSize: Int = 10000,
                           numTips: Int = 50,
                           lastAcceptedTransactionRefSize: Int = 10000,
                           delayinterval: Int = 30) = {
    val checkpointBlocks = Seq.fill[CheckpointBlock](numCheckpointBlocks)(randomCB())
    println("checkpointBlocks")
    val snapshot = Snapshot(Fixtures.randomSnapshotHash, checkpointBlocks.map(_.baseHash))
    println("snapshot")
    val snapshotHashes = Seq.fill[String](2*delayinterval)(Fixtures.randomSnapshotHash)
    val acceptedCBSinceSnapshotCache = Seq.fill(numAcceptedCBSinceSnapshotCache)(CheckpointCache(randomCB()))//todo, remove CheckpointCache, size diff huge 95095683 vs 67001 with acceptedCBSinceSnapshot
    val lastSnapshotHeight = Int.MaxValue/2
    val dummyAddressCacheData = AddressCacheData(1000L, 1000L, Some(1d))
    val addressCacheData = Seq.fill(addressCacheDataSize)((randomAddress, dummyAddressCacheData)).toMap
    println("addressCacheData")
    val tips = Seq.fill[CheckpointBlock](numTips)(randomCB()).map( cb => (cb.baseHash + random.toString, TipData(cb, 2, Height(0L, 100000L)))).toMap
    val lastAcceptedTransactionRef = Seq.fill[String](lastAcceptedTransactionRefSize)(Fixtures.tx.hash + random.toString).map(hash => (hash, LastTransactionRef.empty)).toMap
    val snapshotCache = acceptedCBSinceSnapshotCache ++ acceptedCBSinceSnapshotCache//todo same here with snapshotCache as above
    println("snapshotCache")
    val loadedSnapshotInfo = SnapshotInfo(
      snapshot,
      checkpointBlocks.map(_.baseHash),//todo 5156 matches before crash
      acceptedCBSinceSnapshotCache,
      lastSnapshotHeight,
      snapshotHashes,
      addressCacheData,
      tips,
      snapshotCache,
      lastAcceptedTransactionRef
    )
    loadedSnapshotInfo
  }

  def storeSnapshotInfo(info: SnapshotInfo = generateSnapshotInfo(), storePath : String = smallInfoParts) = {
    val snapshot = info.snapshot
    val acceptedCBSinceSnapshot = info.acceptedCBSinceSnapshot
    val acceptedCBSinceSnapshotCache = info.acceptedCBSinceSnapshotCache
    val lastSnapshotHeight = info.lastSnapshotHeight
    val snapshotHashes = info.snapshotHashes
    val addressCacheData = info.addressCacheData
    val tips = info.tips
    val snapshotCache = info.snapshotCache
    val lastAcceptedTransactionRef = info.lastAcceptedTransactionRef

    SerExt(snapshot).jsonSave(storePath + "/snapshot")
    SerExt(acceptedCBSinceSnapshot).jsonSave(storePath + "/acceptedCBSinceSnapshot")
    SerExt(acceptedCBSinceSnapshotCache).jsonSave(storePath + "/acceptedCBSinceSnapshotCache")
    SerExt(lastSnapshotHeight).jsonSave(storePath + "/lastSnapshotHeight")
    SerExt(snapshotHashes).jsonSave(storePath + "/snapshotHashes")
    SerExt(addressCacheData).jsonSave(storePath + "/addressCacheData")
    SerExt(tips).jsonSave(storePath + "/tips")
    SerExt(snapshotCache).jsonSave(storePath + "/snapshotCache")
    SerExt(lastAcceptedTransactionRef).jsonSave(storePath + "/lastAcceptedTransactionRef")
  }

  def storeSnapshotInfoSer(info: SnapshotInfoSer, storePath: String = infoSerParts) = {
    val snapshot = info.snapshot
    val acceptedCBSinceSnapshot = info.acceptedCBSinceSnapshot
    val acceptedCBSinceSnapshotCache = info.acceptedCBSinceSnapshotCache
    val lastSnapshotHeight = info.lastSnapshotHeight
    val snapshotHashes = info.snapshotHashes
    val addressCacheData = info.addressCacheData
    val tips = info.tips
    val snapshotCache = info.snapshotCache
    val lastAcceptedTransactionRef = info.lastAcceptedTransactionRef

    SerExt(snapshot).jsonSave(storePath + "/snapshot")
    SerExt(acceptedCBSinceSnapshot).jsonSave(storePath + "/acceptedCBSinceSnapshot")
    SerExt(acceptedCBSinceSnapshotCache).jsonSave(storePath + "/acceptedCBSinceSnapshotCache")
    SerExt(lastSnapshotHeight).jsonSave(storePath + "/lastSnapshotHeight")
    SerExt(snapshotHashes).jsonSave(storePath + "/snapshotHashes")
    SerExt(addressCacheData).jsonSave(storePath + "/addressCacheData")
    SerExt(tips).jsonSave(storePath + "/tips")
    SerExt(snapshotCache).jsonSave(storePath + "/snapshotCache")
    SerExt(lastAcceptedTransactionRef).jsonSave(storePath + "/lastAcceptedTransactionRef")
  }

  def loadSnapshotInfoSer(storePath : String = smallInfoParts) = {
    val snapshot = File(storePath + "/snapshot").lines.mkString.x[Array[Byte]]
    val acceptedCBSinceSnapshot = File(storePath + "/acceptedCBSinceSnapshot").lines.mkString.x[Array[Byte]]
    println("acceptedCBSinceSnapshot")
    val acceptedCBSinceSnapshotCache = File(storePath + "/acceptedCBSinceSnapshotCache").lines.mkString.x[Array[Byte]]
    println("acceptedCBSinceSnapshotCache")
    val lastSnapshotHeight = 1//File(storePath + "/lastSnapshotHeight").lines.mkString.x[Int]
    val snapshotHashes = File(storePath + "/snapshotHashes").lines.mkString.x[Array[Byte]]
    val addressCacheData = File(storePath + "/addressCacheData").lines.mkString.x[Array[Byte]]
    println("addressCacheData")
    val tips = File(storePath + "/tips").lines.mkString.x[Array[Byte]]
    val snapshotCache = File(storePath + "/snapshotCache").lines.mkString.x[Array[Byte]]
    val lastAcceptedTransactionRef = File(storePath + "/lastAcceptedTransactionRef").lines.mkString.x[Array[Byte]]
    println("lastAcceptedTransactionRef")

    SnapshotInfoSer(
      snapshot,
      acceptedCBSinceSnapshot,
      acceptedCBSinceSnapshotCache,
      lastSnapshotHeight,
      snapshotHashes,
      addressCacheData,
      tips,
      snapshotCache,
      lastAcceptedTransactionRef
    )
  }

  def loadSnapshotInfo(storePath : String = smallInfoParts) = {
    val snapshot = File(storePath + "/snapshot").lines.mkString.x[Snapshot]
    val acceptedCBSinceSnapshot = File(storePath + "/acceptedCBSinceSnapshot").lines.mkString.x[Seq[String]]
    println("acceptedCBSinceSnapshot")
    val acceptedCBSinceSnapshotCache = File(storePath + "/acceptedCBSinceSnapshotCache").lines.mkString.x[Seq[CheckpointCache]]
    println("acceptedCBSinceSnapshotCache")
    val lastSnapshotHeight = 1//File(storePath + "/lastSnapshotHeight").lines.mkString.x[Int]
    val snapshotHashes = File(storePath + "/snapshotHashes").lines.mkString.x[Seq[String]]
    val addressCacheData = File(storePath + "/addressCacheData").lines.mkString.x[Map[String, AddressCacheData]]
    println("addressCacheData")
    val tips = File(storePath + "/tips").lines.mkString.x[Map[String, TipData]]
    val snapshotCache = File(storePath + "/snapshotCache").lines.mkString.x[Seq[CheckpointCache]]
    val lastAcceptedTransactionRef = File(storePath + "/lastAcceptedTransactionRef").lines.mkString.x[Map[String, LastTransactionRef]]
    println("lastAcceptedTransactionRef")

    SnapshotInfo(
      snapshot,
      acceptedCBSinceSnapshot,
      acceptedCBSinceSnapshotCache,
      lastSnapshotHeight,
      snapshotHashes,
      addressCacheData,
      tips,
      snapshotCache,
      lastAcceptedTransactionRef
    )
  }





//  "KryoSerializer" should "serializing within buffer size" in {
//      val snap = generateSnapshotInfo()
////      storeSnapshotInfo(snap, InfoPartsDeflateSerializer)
////    val loadedSnapshotInfo = loadSnapshotInfo(smallInfoParts)
//    val deSer = Try {KryoSerializer.serializeAnyRef(snap)}
//    deSer.map { b =>
//      SerExt(b).jsonSave(InfoPartsDeflateSerializer + "/snapshot")
//    }
//    assert(deSer.isSuccess)
//  }
//
//  "KryoSerializer" should "fail when serializing beyond buffer size" in {
////    val loadedSnapshotInfo = loadSnapshotInfo(lrgInfoParts)
//    val snap = generateSnapshotInfo()
//    val loadedSnapshotInfo = File(InfoPartsDeflateSerializer + "/snapshot").lines.mkString.x[Array[Byte]]
//
////    val ser = Try {KryoSerializer.serializeAnyRef(loadedSnapshotInfo)}
//    println("loadedSnapshotInfo" + loadedSnapshotInfo.length)
//    val deSer = Try {KryoSerializer.deserializeCast[SnapshotInfo](loadedSnapshotInfo)}
//    val chunkedSer = Try{EdgeProcessor.toSnapshotInfoSer(deSer.get)}
//    storeSnapshotInfoSer(chunkedSer.get, InfoPartsDeflateSerializer)
//    assert(deSer.isFailure)
//  }
//
//  "KryoSerializer" should "succeed when serializing SnapshotInfoSer" in {
//    val loadedSnapshotInfo: SnapshotInfo = loadSnapshotInfo(smallInfoParts)
//    val infoSer = EdgeProcessor.toSnapshotInfoSer(loadedSnapshotInfo)
//    storeSnapshotInfoSer(infoSer)
//    val ser2 = Try {KryoSerializer.serializeAnyRef(loadedSnapshotInfo)}
//    println("ser initial" + ser2.get.size)
//
//    val ser = Try {KryoSerializer.serializeAnyRef(infoSer)}
//    println("ser" + ser.get.size)
//
//    val deSer = Try {KryoSerializer.deserializeCast[SnapshotInfoSer](ser.get)}
//    val deSerAI = EdgeProcessor.toSnapshotInfo(deSer.get)
//    assert(deSerAI == loadedSnapshotInfo)
//  }
  /*
  "KryoSerializer" should "round trip serialize and deserialize SerializedUDPMessage" in {

    val message = SerializedUDPMessage(ByteString("test".getBytes), 1, 1, 1)

    val serialized = serialize(message)

    val deserialized = deserialize(serialized)

    assert(message == deserialized)

    val testBundle = Gossip(TestHelpers.createTestBundle())

    assert(testBundle.event.valid)

    val messages = serializeGrouped(testBundle)

    val messagesSerialized = messages.map(serialize(_))

    val messagesDeserialized: Seq[SerializedUDPMessage] = messagesSerialized.map(deserialize(_).asInstanceOf[SerializedUDPMessage])

    val sorted = messagesDeserialized.sortBy(f => f.packetGroupId).flatMap(_.data).toArray

    val deserializedSorted = deserialize(sorted).asInstanceOf[Gossip[Bundle]]

    assert(testBundle == deserializedSorted)

    assert(deserializedSorted.event.valid)
  }
 */

}
