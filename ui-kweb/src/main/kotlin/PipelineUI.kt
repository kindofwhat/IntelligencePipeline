import datatypes.DataRecord
import io.kweb.Kweb
import io.kweb.dom.element.Element
import io.kweb.dom.element.creation.ElementCreator
import io.kweb.dom.element.creation.tags.*
import io.kweb.dom.element.events.on
import io.kweb.dom.element.new
import io.kweb.plugins.fomanticUI.fomanticUIPlugin
import io.kweb.plugins.foundation.foundation
import io.kweb.plugins.foundation.grid.column
import io.kweb.plugins.foundation.grid.row
import io.kweb.plugins.fomanticUI.*
import io.kweb.state.KVar
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.stringify
import mu.KotlinLogging
import orientdb.OrientDBPipeline
import participants.DirectoryIngestor
import pipeline.IIntelligencePipeline
import kotlin.coroutines.CoroutineContext

@ObsoleteCoroutinesApi
@ImplicitReflectionSerializer
object PipelineUI : CoroutineScope {
    lateinit var job: Job

    override val coroutineContext: CoroutineContext
        get() = job + Dispatchers.Default


    private val logger = KotlinLogging.logger {}

    private val dataRecords = KVar(mutableListOf<DataRecord>())

    var pipeline: OrientDBPipeline? = null
    fun main(args: Array<String>) {
        job = Job()
        logger.debug("Starting GUI")
        Kweb(port = 9090, refreshPageOnHotswap = true, debug = true, plugins = listOf(fomanticUIPlugin, foundation)) {

            doc.body.new {
                val creator = this
                div(fomantic.ui.two.column.left.grid).new {
                    pipelineActions(creator)
                }
            }
        }
    }

    private fun pipelineResults(elementCreator: ElementCreator<*>): Unit {
        //dataRecords.value.clear()
        pipeline?.liveSubscriber { datarecord ->
            elementCreator.foundation.row().new {
                column().text(datarecord.name)
                div(mapOf("style" to "white-space:pre")).text(Json.indented.stringify(datarecord))
            }
            //dataRecords.value.add(datarecord)

        }
    }

    private fun pipelineActions(creator: ElementCreator<*>): Element {

        val container = creator.div(fomantic.column).text("Pipeline")

        container.new() {
            form(fomantic.ui.form).new {
                val docDir = input("Doc Dir", "/home/christian/Dokumente", this)
                val outDir = input("Out dir", "/tmp/pipeline", this)
                val connectionUrl = input("Connection Url", "remote:localhost", this)
                val dbName = input("DB Name", "ip", this)
                val user = input("user", "root", this)
                val password = input("Password", "ip", this)

                button(fomantic.ui.button).text("Start").apply {
                    on.click {
                        launch {
                            pipeline = createPipeline(
                                    docDir?.getValue()?.await()?: "",
                                    outDir?.getValue()?.await()?: "",
                                    connectionUrl?.getValue()?.await()?: "",
                                    dbName?.getValue()?.await()?: "",
                                    user?.getValue()?.await()?: "",
                                    password?.getValue()?.await()
                                    ?: "", listOf(DirectoryIngestor(docDir?.getValue()?.await() ?: "")))
                            pipeline?.run()

                        }
                    }
                }
                button(fomantic.ui.button).text("Stop").apply {
                    on.click {
                        pipeline?.stop()
                    }
                }
                button(fomantic.ui.button).text("Query").apply {
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
        elementCreator.div(fomantic.ui.field).new {
            result = input(InputType.text, name.toLowerCase(), placeholder = placeholder, initialValue = placeholder)
            label(for_ = result).text(name.capitalize())

        }
        return result;
    }


}

