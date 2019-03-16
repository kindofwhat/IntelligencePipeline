import datatypes.DataRecord
import io.kweb.Kweb
import io.kweb.dom.element.Element
import io.kweb.dom.element.creation.ElementCreator
import io.kweb.dom.element.creation.tags.*
import io.kweb.dom.element.events.on
import io.kweb.dom.element.new
import io.kweb.plugins.foundation.foundation
import io.kweb.plugins.semanticUI.semantic
import io.kweb.plugins.foundation.grid.*
import io.kweb.plugins.semanticUI.semanticUIPlugin
import io.kweb.shoebox.Shoebox
import io.kweb.state.KVal
import io.kweb.state.KVar
import io.kweb.state.persistent.render
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.future.await
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.stringify
import mu.KotlinLogging
import participants.DirectoryIngestor
import pipeline.IIntelligencePipeline
import java.util.*
import kotlin.coroutines.CoroutineContext

@ObsoleteCoroutinesApi
@ImplicitReflectionSerializer
object PipelineUI : CoroutineScope {
    lateinit var job: Job

    override val coroutineContext: CoroutineContext
        get() = job + Dispatchers.Default


    private val logger = KotlinLogging.logger {}

    private val dataRecords = KVar(mutableListOf<DataRecord>())

    var pipeline: IIntelligencePipeline? = null
    fun main(args: Array<String>) {
        job = Job()
        logger.debug("Starting GUI")
        Kweb(port = 9090, refreshPageOnHotswap = true, debug = true, plugins = listOf(semanticUIPlugin, foundation)) {

            doc.body.new {
                val creator = this
                div(semantic.ui.two.column.left.grid).new {
                    pipelineActions(creator)
                }
            }
        }
    }

    private fun pipelineResults(elementCreator: ElementCreator<*>): Unit {
        dataRecords.value.clear()
        launch {
            for (datarecord in pipeline?.dataRecords("ui${System.currentTimeMillis()}")!!) {
                elementCreator.foundation.row().new {
                    column().text(datarecord.name)
                    div(mapOf("style" to "white-space:pre")).text(Json.indented.stringify(datarecord))
                }
                dataRecords.value.add(datarecord)
            }
        }
    }

    private fun pipelineActions(creator: ElementCreator<*>): Element {

        val container = creator.div(semantic.column).text("Pipeline")

        container.new() {
            form(semantic.ui.form).new {
                val bootstrap = input("Bootstrap", "localhost:29092", this)
                val stateDir = input("StateDir", "/tmp", this)
                val documentDir = input("DocumentDir", "/tmp/testresources", this)

                button(semantic.ui.button).text("Start").apply {
                    on.click {
                        launch {
                            pipeline = createPipeline(bootstrap?.getValue()?.await()
                                    ?: "", stateDir?.getValue()?.await()
                                    ?: "", listOf(DirectoryIngestor(documentDir?.getValue()?.await() ?: "")))
                            pipeline?.run()

                        }
                    }
                }
                button(semantic.ui.button).text("Stop").apply {
                    on.click {
                        pipeline?.stop()
                    }
                }
                button(semantic.ui.button).text("Query").apply {
                    on.click {
                        pipelineResults(creator)
                    }
                }
            }
        }
        return container
    }


    private fun input(name: String, placeholder: String, elementCreator: ElementCreator<FormElement>): InputElement? {
        var result: InputElement? = null;
        elementCreator.div(semantic.ui.field).new {
            result = input(InputType.text, name.toLowerCase(), placeholder = placeholder, initialValue = placeholder)
            label(for_ = result).text(name.capitalize())

        }
        return result;
    }


}

