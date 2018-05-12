package com.example

import pl.treksoft.kvision.form.FormPanel
import pl.treksoft.kvision.form.FormPanel.Companion.formPanel
import pl.treksoft.kvision.form.string
import pl.treksoft.kvision.form.text.Text
import pl.treksoft.kvision.html.Button
import pl.treksoft.kvision.panel.SimplePanel
import pl.treksoft.kvision.utils.px
import com.egorzh.networkinkt.*
import datatypes.DocumentRepresentation
import kotlinx.coroutines.experimental.async
import kotlinx.serialization.json.JSON


data class PipelineModel(val bootstrap: String?)

object ActionPanel : SimplePanel() {

    private val formPanel: FormPanel<PipelineModel>
    init {
        padding = 10.px

        formPanel = formPanel {
            PipelineModel(it.string("bootstrap"))
        }.apply {
            add(PipelineModel::bootstrap, Text(label = "Start IP with Bootstrap value"), required = true)
            add(Button("OK").onClick {
                async {
                    println("result: " + JSON.parse<DocumentRepresentation>(HTTP.get("http://localhost:7000/test").getText()) )
                }
                val data: PipelineModel = this@apply.getData()
                println("Bootstrap: ${data.bootstrap}")
            })
        }


    }
}
