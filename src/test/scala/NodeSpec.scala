import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActors, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
  * Created by Wyatt on 11/10/17.
  */

class NodeSpec() extends TestKit(ActorSystem("NodeSpec")) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Constellation Node" must {

    "Return Receipt message" in {
      val testTx = Transaction(
        Array(),
        1L,
        "id",
        "counterPartyPubKey",
        "message",
        "signature")
      val node = system.actorOf(Props(new Node(system, Nil)), name = "ConstellationNode")
      node ! testTx
      expectMsg(s"received: ${testTx.toString}")
    }

  }
}
