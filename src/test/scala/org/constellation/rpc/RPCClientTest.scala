package org.constellation.rpc

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.concurrent.ExecutionContextExecutor
import constellation._
import org.constellation.Fixtures
import org.constellation.primitives.Transaction.Transaction
import org.constellation.utils.{RPCClient, TestNode}

class RPCClientTest extends FlatSpec with BeforeAndAfterAll {

  // It's useful to change the port in case another node is already running.
  // This strictly speaking isn't necessary for the full unit tests, but
  // we can use this to remove the serial test running by modifying other tests.
  // Leaving it here as an example until that is fixed
  val conf: Config = ConfigFactory.empty().withValue(
    "akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(2556)
  )

  implicit val system: ActorSystem = ActorSystem("BlockChain", conf)
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val appNode = TestNode()

  val seedRPC = new RPCClient(port=appNode.httpPort)

  "SendTX" should "send a transaction and receive it back" in {

    val tx = Fixtures.tx //Transaction("hashpointer", "id", 1L, "key", "key2", 5L, "sig")
    val response = seedRPC.sendTx(tx)
    val tx2 = seedRPC.read[Transaction](response.get()).get()
    assert(tx == tx2)

  }

  "GetBalance" should "retrieve a balance properly" in {

    val response = seedRPC.getBalance(Fixtures.publicKey)

    // TODO: add balance back
  //  val balance = seedRPC.read[Balance](response.get()).get()
  //  assert(balance.balance == 0L)
    // TODO: make a fake account with a balance and verify retrieval works.

  }

  override def afterAll() {
    appNode.system.terminate()
  }

}