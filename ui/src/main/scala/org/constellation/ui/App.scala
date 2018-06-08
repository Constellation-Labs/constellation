package org.constellation.ui


import org.scalajs.dom
import org.scalajs.dom.raw.{HTMLDivElement, HTMLElement, HTMLInputElement, MouseEvent}
import rx.ops.{DomScheduler, Timer}

import scala.scalajs.js
import scala.scalajs.js.{Date, JSApp}
import scala.scalajs.js.annotation.JSExport
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case class Metrics(metrics: Map[String, String])


object App extends JSApp {

  @JSExport
  def main(): Unit = {

    import scalatags.JsDom.all._
    println("hello world")

    val dash = dom.document.body.appendChild(div(id := "dash").render).asInstanceOf[HTMLDivElement]

    val forms = dash.appendChild(div(id := "forms").render).asInstanceOf[HTMLDivElement]

    val formsSeq = Seq(
      form(
        action := "/setKeyPair",
        "Set node KeyPair:  ",
        input(
          `type` := "text", name := "keyPair", value := ""
        )
      ),
      form(
        action := "/submitTX",
        "Submit transaction - address:  ",
        input(
          `type` := "text", name := "address", value := ""
        ),
        "amount:  ",
        input(
          `type` := "text", name := "amount", value := ""
        ),
        button(
          `type` := "submit",
          "Submit"
        )
      )
    )

    formsSeq.foreach{ f => forms.appendChild(f.render)}


    val metricsDiv = dash.appendChild(div(id := "metrics").render).asInstanceOf[HTMLDivElement]

    implicit val scheduler: DomScheduler = new DomScheduler()

    val heartBeat = Timer(3000.millis)

    heartBeat.foreach( _ => {
      XHR.get[Metrics]({zo: Metrics =>
        metricsDiv.innerHTML = ""
        val zmt = zo.metrics.toSeq.map{
          case ("address", v) =>
            "address" -> Seq(a(href := s"/address/$v", v))
          case ("peers", v) =>
            val vs = v.split(" --- ").map{
              pr =>
                val fullStr = pr.split("API: ")
                val address = fullStr.last.split(" ").head
                div(fullStr.head,  a(href := address, address))
            }.toSeq
            "peers" -> vs
          case ("last10TXHash", v) =>
            "last10TXHash" -> v.split(",").map{ addr =>
              div(a(href := s"/txHash/$addr", addr))
            }.toSeq
          case (k,v) => k -> Seq(div(v))
        }
        val mets = zmt.sortBy(_._1).map{
          case (k,v) =>
            tr(
              td(k),
              td(v)
            )
        }
        val tbl = table(
          tr(
            th("Metric Name"),
            th("Metric Value")
          ),
          mets
        ).render
        metricsDiv.appendChild(tbl)
        //  println(z)
      }, "/metrics")
    }
    )


  }

}