package com.example

import pl.treksoft.kvision.hmr.ApplicationBase
import pl.treksoft.kvision.panel.Root
import pl.treksoft.kvision.panel.SimplePanel.Companion.simplePanel
import pl.treksoft.kvision.panel.SplitPanel.Companion.splitPanel
import pl.treksoft.kvision.require
import pl.treksoft.kvision.utils.vh

class App : ApplicationBase {

    override fun start(state: Map<String, Any>) {
        model.loadAddresses()
        Root("kvapp") {
            simplePanel {
                height = 100.vh
                add(ActionPanel)
            }
        }
    }

    override fun dispose(): Map<String, Any> {
        return mapOf()
    }

    companion object {
        val css = require("./css/kvapp.css")
    }
}
