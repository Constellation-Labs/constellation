//package org.constellation
//
//import java.security.KeyPair
//
//import akka.NotUsed
//import akka.actor.Status.Success
//import akka.actor.{ActorRef, ActorSystem}
//import akka.stream.{ActorMaterializer, OverflowStrategy}
//import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
//import org.constellation.wallet.KeyUtils.makeKeyPair
//import org.reactivestreams.Publisher
//import org.reactivestreams.Subscriber
//
//import scala.concurrent.Future
///**
//  * Created by Wyatt on 5/10/18.
//  */
//
///**
//  * Equivalent to a ring buffer, provide buffering when accessing chain state and signing data.
//  * @param keyPair
//  * @param system
//  */
//class NodeStateManager(val keyPair: KeyPair = makeKeyPair(), system: ActorSystem,
//                       router: ActorRef,
//                       eventStream: Flow[Int, Sheaf, NotUsed]
//                      )(implicit val materializer: ActorMaterializer) {
//
//  /**
//    * Pipes messages sent to ActorRef into async buffer, will need mapAsync when returning futures (ask's to chain state manager)
//    */
// val buffer =  Source.actorRef[Int](100, OverflowStrategy.backpressure).map(embed).async.to(Sink.actorRef(router, Success)).run()
//
//  /**
//    * 'embed' event into our local manifold by 'lifting' into cell functor.
//    * @param event
//    * @return
//    */
//  def embed(event: Int): Sheaf = Cell.ioF(Sheaf())
//}