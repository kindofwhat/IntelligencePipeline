import io.kweb.Kweb
import io.kweb.dom.element.Element
import io.kweb.dom.element.creation.ElementCreator
import io.kweb.dom.element.creation.tags.*
import io.kweb.dom.element.events.on
import io.kweb.dom.element.new
import io.kweb.plugins.semanticUI.semantic
import io.kweb.plugins.semanticUI.semanticUIPlugin
import mu.KotlinLogging



object PipelineUI {
    private val logger = KotlinLogging.logger {}

    fun main(args: Array<String>) {
        var count = 0

        Kweb(port = 8088, debug = true, plugins = listOf(semanticUIPlugin)) {
            doc.body.new {
                div(semantic.ui.two.column.left.grid).new {
                    pipelineElement(this)
                }
            }
        }
    }
}

fun pipelineElement(creator:ElementCreator<*>):Element {
    val container = creator.div(semantic.column).text("Pipeline")

    container.new() {
        form().new {
            val bootstrap = input(InputType.text,"bootstrap",placeholder = "localhost:9092")
            label(for_ = bootstrap).text("Bootstrap")
        }
    }
    return container
}