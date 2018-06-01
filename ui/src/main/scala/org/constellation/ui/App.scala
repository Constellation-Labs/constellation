package org.constellation.ui


import org.scalajs.dom
import org.scalajs.dom.raw.{HTMLDivElement, HTMLElement, HTMLInputElement, MouseEvent}
import rx.ops.{DomScheduler, Timer}

import scala.scalajs.js
import scala.scalajs.js.{Date, JSApp}
import scala.scalajs.js.annotation.JSExport
import scala.util.Try
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case class Metrics(metrics: Map[String, String])


object App extends JSApp {

  @JSExport
  def main(): Unit = {

    import scalatags.JsDom.all._
    println("hello world")

    val metricsDiv = dom.document.body.appendChild(div(id := "metrics").render).asInstanceOf[HTMLDivElement]


    implicit val scheduler: DomScheduler = new DomScheduler()

    val heartBeat = Timer(1000.millis)

    heartBeat.foreach( _ => {
      XHR.get[Metrics]({z: Metrics =>
        metricsDiv.innerHTML = ""
        val mets = z.metrics.toSeq.sortBy(_._1).map{
          case (k,v) =>
            tr(td(k), td(v))
        }
        val tbl = table(
          tr(
            th("Metric Name"),
            th("Metric Value")
          ),
          mets
        ).render
        metricsDiv.appendChild(tbl)
        println(z)
      }, "/metrics")
    }
    )


  }

}