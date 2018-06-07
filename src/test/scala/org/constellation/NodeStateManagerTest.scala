package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

/**
  * Created by Wyatt on 6/7/18.
  */
class NodeStateManagerTest {
  implicit val system: ActorSystem = ActorSystem("NodeStateManagerTest")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
}
