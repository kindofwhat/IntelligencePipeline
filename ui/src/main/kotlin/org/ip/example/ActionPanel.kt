package org.ip.example

import pl.treksoft.kvision.form.FormPanel.Companion.formPanel
import pl.treksoft.kvision.form.string
import pl.treksoft.kvision.form.text.Text
import pl.treksoft.kvision.html.Button
import pl.treksoft.kvision.panel.SimplePanel
import pl.treksoft.kvision.utils.px
import com.egorzh.networkinkt.*
import commands.StartPipeline
import kotlin.coroutines.async
import kotlinx.serialization.json.JSON
import org.w3c.dom.WebSocket


//data class PipelineModel(val bootstrap: String?)

object ActionPanel : SimplePanel() {

    init {
        padding = 10.px

        formPanel {
            StartPipeline(it.string("bootstrap") ?: "", it.string("stateDirectory")
                    ?: "", it.string("scanDirectory") ?: "")
        }.apply {
            add(StartPipeline::bootstrap, Text(label = "Start IP with Bootstrap value"), required = true)
            add(StartPipeline::stateDirectory, Text(label = "State Directory"), required = true)
            add(StartPipeline::scanDirectory, Text(label = "Scan Directory"), required = true)

            add(Button("START").onClick {
                val data: StartPipeline = this@apply.getData()
                async {
                    HTTP.post("http://localhost:7000/startPipeline",  body = JSON.stringify(data)).send()
                }
            })
            add(Button("STOP").onClick {
                async {
                    HTTP.post("http://localhost:7000/stopPipeline").send()
                }
            })

            val ws = WebSocket("ws://localhost:7000/websocket/datarecord")

            ws.onmessage = { event -> println(JSON.parse(event.toString()))}

        }


    }
}
