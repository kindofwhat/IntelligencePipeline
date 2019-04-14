package info.ip.ui

import com.github.mvysny.karibudsl.v10.*
import com.vaadin.flow.component.Component
import com.vaadin.flow.component.ComponentEvent
import com.vaadin.flow.component.HasComponents
import com.vaadin.flow.component.UI
import com.vaadin.flow.component.dependency.HtmlImport
import com.vaadin.flow.component.html.Div
import com.vaadin.flow.component.html.ListItem
import com.vaadin.flow.component.html.UnorderedList
import com.vaadin.flow.component.notification.Notification
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.page.BodySize
import com.vaadin.flow.component.page.Push
import com.vaadin.flow.component.page.Viewport
import com.vaadin.flow.component.tabs.Tab
import com.vaadin.flow.component.tabs.Tabs
import com.vaadin.flow.router.Route
import com.vaadin.flow.server.ErrorHandler
import com.vaadin.flow.theme.Theme
import com.vaadin.flow.theme.lumo.Lumo
import createPipeline
import datatypes.DataRecord
import datatypes.DocumentRepresentation
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.stringify
import participants.DirectoryIngestor
import participants.TikaMetadataProducer
import pipeline.IIntelligencePipeline
import kotlin.coroutines.CoroutineContext


sealed class PipelineMessage
class PipelineCreateMessage(val connection: String, val dbName: String, val user: String, val password: String, val scanDirectory:String): PipelineMessage()
class PipelineStartMessage : PipelineMessage()
class PipelineStopMessage : PipelineMessage()

sealed class DocRepMessage
class NewDocumentRepresentation(val documentRepresentation: DocumentRepresentation) : DocRepMessage()

class Pipeline<T : Component>(source: T, fromClient: Boolean = false) : ComponentEvent<T>(source, fromClient)
/**
 * The main view contains a button and a template element.
 */
@UseExperimental(ObsoleteCoroutinesApi::class)
@ImplicitReflectionSerializer
@BodySize(width = "100vw", height = "100vh")
@HtmlImport("frontend://styles.html")
@Route("")
@Viewport("width=device-width, minimum-scale=1.0, initial-scale=1.0, user-scalable=yes")
@Theme(Lumo::class)
@Push
class MainView : VerticalLayout(), CoroutineScope {
    /**
     * I must use the [SupervisorJob] here; regular [Job] would cancel itself if any of the child coroutines failed, and that would
     * prevent launching more coroutines.
     */
    private val uiCoroutineScope = SupervisorJob()
    private val uiCoroutineContext = vaadin( UI.getCurrent())
    override val coroutineContext: CoroutineContext
        get() = uiCoroutineContext + uiCoroutineScope


    val tabPages = mutableListOf<Component>()
    var dataRecordChannel = Channel<DataRecord>()

    val actionChannel = Channel<PipelineMessage>()


    init {

        ErrorHandler { event ->
            Notification("Internal error ${event.throwable}", 3000,
                    com.vaadin.flow.component.notification.Notification.Position.MIDDLE).open()
        }
        launch(this.uiCoroutineScope) {
            var myPipeline: IIntelligencePipeline? = null
            actionChannel.consumeEach { msg ->
                when (msg) {
                    is PipelineCreateMessage -> {
                        val job = async {
                            val pipeline: IIntelligencePipeline
                            pipeline = createPipeline(msg.connection, msg.dbName, msg.user,msg.password,
                                    listOf(DirectoryIngestor(msg.scanDirectory)))
                            pipeline.registerSideEffect("ui") { key, value ->
                                    dataRecordChannel.sendBlocking(value)
                            }
                            pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))
                            pipeline
                        }
                        myPipeline = job.await()
                    }

                    is PipelineStartMessage -> myPipeline?.run()
                    is PipelineStopMessage -> myPipeline?.stop()
                }


            }
        }

        verticalLayout {
            content { align(left, top) }
            tabs {
                addSelectedChangeListener {
                    tabPages.forEachIndexed() { i, u ->
                        u.isVisible = i == selectedIndex
                    }
                }
                orientation = Tabs.Orientation.HORIZONTAL
                tab { label("Pipeline") }
                tab { label("Docs") }
                selectedIndex = 0
            }
            verticalLayout {
                val page = verticalLayout {

                    content { align(left, evenly) }
                    val connection = textField("Connection URL") {
                        value = "remote:"
                    }
                    val dbName = textField("DB Name") {
                        value = "ip"
                    }
                    val user = textField("User") {
                        value = "user"
                    }
                    val password = passwordField("Password") {
                        value = ""
                    }
                    val scan = textField("Scan Directory") {
                        value = "/tmp"
                    }
                    button("Start Pipeline!").onLeftClick { event ->
                        async {
                            actionChannel.send(PipelineCreateMessage(connection.value, dbName.value, user.value, password.value, scan.value))
                            actionChannel.send(PipelineStartMessage())
                        }
                    }
                    button("Stop Pipeline").onLeftClick { event ->
                        async {
                            actionChannel.send(PipelineStopMessage())
                        }
                    }
                    isVisible = true
                }
                tabPages.add(page)
                val pipelineLayout = verticalLayout {
                    val parentLayout = this
                    grid<DataRecord> {
                        flexBasis = "auto"

                        val items = mutableSetOf<DataRecord>()
                        addColumn(DataRecord::name).setHeader("Name")
                        async {
                            dataRecordChannel.consumeEach { dataRecord ->
                                try {
                                    println("datarecord: $dataRecord")
                                    items += dataRecord
                                    setItems(items)
                                } catch (e: Exception) {
                                    println(e)
                                }
                            }
                        }
                        addSelectionListener { selected ->
                            if (selected.firstSelectedItem.isPresent) {
                                val dataRecord = selected.firstSelectedItem.get()
                                val dialog = parentLayout.dialog {
                                    verticalLayout {
                                        label(JSON(unquoted = true, indented = true).stringify(dataRecord)) {
                                            style.set("white-space", "pre")
                                        }
                                    }
                                }
                                dialog.open()
                            }
                        }
                    }
                    isVisible = false
                }
                tabPages.add(pipelineLayout)

            }
        }
    }
}


fun HasComponents.ul(block: UnorderedList.() -> Unit = {}) = init(UnorderedList(), block)
fun HasComponents.li(text: String, block: ListItem.() -> Unit = {}) = init(ListItem(text), block)
fun HasComponents.li(block: ListItem.() -> Unit = {}) = init(ListItem(), block)
fun HasComponents.tabs(block: Tabs.() -> Unit = {}) = init(Tabs(), block)
fun HasComponents.tab(block: Tab.() -> Unit = {}) = init(Tab(), block)
fun HasComponents.div(block: Div.() -> Unit = {}) = init(Div(), block)