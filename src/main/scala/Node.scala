/**
  * Created by Wyatt on 11/10/17.
  */
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer

/**
  * Created by Wyatt on 10/31/17.
  */

/**
  * Currently assumes all members of system/peers are facilitators in each round
  * @param system system ref
  */
class Node(system: ActorSystem, val peers: List[ActorRef]) extends Actor with LazyLogging {
  val bufferThreshold = 10 //TODO implement dynamic scaling as in HoneybadgerBFT
  var blockNumber = 1
  val txBuffer = new ListBuffer[Transaction]()
  val consensusBlock = collection.mutable.Set[Block]()
  val facilitatorVotes = collection.mutable.HashMap[ActorRef, Option[Block]]()
    peers.foreach(peer => facilitatorVotes += (peer -> None))

  def receive: PartialFunction[Any, Unit] = {
    case string: String => println(string)

    case tx: Transaction =>
      txBuffer.append(tx)
      sender() ! s"received: ${tx.toString}"

    case cm: CheckpointMessage =>
      cm.txs.foreach(consensusBlock.add)
      consensusBlock.add(cm)//TODO hash cm first, this is for reputation score, more tx's more noOs
      facilitatorVotes(sender()) = Some(cm)
      if (facilitatorVotes.values.forall(_.isDefined))
        performConsensus()


    case cb: CheckpointBlock =>
      /*
      This is where outputs from a consensus round get stored. If hashes are different the nodes do not get recycled noOs. If a response is not received, that node gets no noOs.
      These cb blocks need to get stored to disk and then can be reconstructed to get history of consensus which gives us the chain and votes which gives us recycled noOs fee distribution.
      TODO logic for chain updates based on receipt of checkpoint blocks, this is for cases where seeder is sending chain to a facilitator node
       */
      consensusBlock.add(cb)
  }

  /*
    * We probably want this happening in it's own actor
   */
  def performConsensus(): Unit = {
    val consensusResult = CheckpointBlock(
      Array(),
      bufferThreshold,
      facilitatorVotes,
      1L,
      "stubbedConsensus")

    peers.foreach(_ ! consensusResult)
    println(facilitatorVotes.toString())
    blockNumber += 1
  }

}

object Node {
  /**
    * The basic consensus (for full nodes) works as follows
    * 1. Transactions are broadcasted to peers and buffered.
    *
    * 2. (in parallel)
    *   a) As the buffer threshold gets hit for peers, those with buffers full (eventually, ranked according to reputation rules)
    *     for consensus get chosen to facilitate.
    *   b) Buffers empty and start getting filled with new tx's.
    *
    * 3. Round robin union of all tx's amongst facilitators, then result gets hashed and stored into CheckpointBlock,
    *   facilitators all vote on final CheckpointBlock, byzantine participants are recognized by looking in history,
    *   recycled noOs is dynamically calculated by looking at chain history.
    *
    * 4. CheckpointBlock sent back to non facilitators, chain cache updated for nodes participating, history updated for seeding nodes.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("localP2P")
    val node = system.actorOf(Props(new Node(system, Nil)), name = "ConstellationNode")
    node ! "science bitch"
  }
}