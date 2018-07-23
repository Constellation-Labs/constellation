package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.constellation.ClusterTest.getPodMappings
import org.constellation.util.APIClient

import scala.concurrent.ExecutionContextExecutor

object RestartCluster extends TestKit(ActorSystem("ClusterTest")){

  def main(args: Array[String]): Unit = {

    implicit val materialize: ActorMaterializer = ActorMaterializer()

    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    constellation.makeKeyPair()

    val clusterId = sys.env.getOrElse("CLUSTER_ID", "constellation-app-ryle")

    val mappings = getPodMappings(clusterId)

    mappings.foreach{println}

    val ips = mappings.map{_.externalIP}

    val rpcs = ips.map{ ip =>
      val r = new APIClient(ip, 9000)
      r
    }

/*
    val sim = new Simulation(rpcs)
    sim.setIdLocal()
*/


    rpcs.foreach {
      _.get("restart")
    }
    Thread.sleep(5000)

    rpcs.foreach {
      _.get("restart")
    }
    Thread.sleep(5000)

    rpcs.foreach {
      _.get("restart")
    }

    Thread.sleep(5000)
/*

//http://35.238.29.152:9000/

    val n = rpcs.filter{_.host == "35.238.29.152"}.head //35.193.103.124"

    println(n.setExternalIP())

    val others = rpcs.filter{_ != n}

    others.foreach{ o =>
      println(n.addPeer(o.udpAddress))
    }
*/

    system.terminate()



  }
}