package org.constellation

import java.security.KeyPair

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.constellation.wallet.KeyUtils.makeKeyPair
/**
  * Created by Wyatt on 5/10/18.
  */

/**
  * Idea is to provide buffering when accessing chain state and signing data.
  * TODO use Source in p2p actor for buffering data from NodeStateManager if we want routing hapenning in p2p, it may come from validation.
  * @param keyPair
  * @param system
  */
class NodeStateManager(p2pActor: ActorRef, val keyPair: KeyPair = makeKeyPair(), system: ActorSystem,
                       eventStream: Source[Int, NotUsed] = Source(1 to 100))(implicit val materializer: ActorMaterializer) {
  /**
    * 'embed' event into our local manifold by 'lifting' into cell functor.
    * @param event
    * @return
    */
  def embed(event: Int): Cell[Sheaf] = Bundle(Sheaf())

  eventStream.runForeach { event =>
    val embeddedEvent = embed(event)
    p2pActor ! embeddedEvent
  }(materializer)
}