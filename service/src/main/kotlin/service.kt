import datatypes.DocumentRepresentation
import io.javalin.Javalin
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.parse
import kotlinx.serialization.stringify
import participants.*
import participants.file.*
import orientdb.OrientDBPipeline
import pipeline.IIntelligencePipeline

@ImplicitReflectionSerializer
var pipeline: IIntelligencePipeline? = null //= KafkaIntelligencePipeline()


data class StartPipeline(val connection: String, val dbName: String, val user: String, val password: String, val scanDirectory:String)
@ImplicitReflectionSerializer
fun main(args: Array<String>) {

    val app = Javalin.create().apply {
        port(7000)
        exception(Exception::class.java) { e, ctx -> e.printStackTrace() }
        error(404) { ctx -> ctx.json("not found") }
        enableCorsForAllOrigins()
    }.start()

    with(app) {

        get("/") { ctx -> ctx.result("Hello World!!") }
        get("/test") { ctx -> ctx.result(JSON.stringify(DocumentRepresentation("path", "test"))) }
        post("/startPipeline") { ctx ->
            startPipeline(JSON.parse(ctx.body() ?: "") as StartPipeline, app)
        }
        post("/stopPipeline") { ctx ->
            stopPipeline()
        }
        createWesocketClient(app)
    }


}

@ImplicitReflectionSerializer
fun stopPipeline() {
    pipeline?.stop()
}

@ObsoleteCoroutinesApi
@ImplicitReflectionSerializer
fun startPipeline(command: StartPipeline, app: Javalin) {
    pipeline = createPipeline(command.connection,command.dbName,command.user,command.password,
            listOf(DirectoryIngestor(command.scanDirectory)), emptyList(), app)
    pipeline?.run()

}

@ImplicitReflectionSerializer
fun createWesocketClient(app:Javalin): Javalin? {
    return app.ws("/websocket/datarecord") { ws ->
        ws.onConnect { session ->
            println("Connected to datarecord websocket")
            runBlocking {
                coroutineScope {
                    launch {
                        pipeline?.dataRecords("testPipeline_read${System.currentTimeMillis()}")?.consumeEach { dataRecord ->
                            session.remote.sendString(JSON.stringify(dataRecord))
                        }
                    }

                }
            }
        }
        ws.onClose { session, statusCode, reason -> println("Closed") }
        ws.onError { session, throwable -> println("Errored $throwable") }
        ws.onMessage { session, msg -> }
    }
}


@ImplicitReflectionSerializer
fun createPipeline(connection: String, dbName: String, user: String, password: String, ingestors: List<PipelineIngestor>, producers: List<MetadataProducer>, app: Javalin): IIntelligencePipeline {
    val pipeline = OrientDBPipeline(connection, dbName,user, password)
    ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor) }
    producers.forEach { producer -> pipeline.registerMetadataProducer(producer) }
    pipeline.registry.register(FileOriginalContentCapability())

    pipeline.registry.register(FileTxtOutputProvider("out/test"))
    pipeline.registry.register(FileTxtStringProvider("out/test"))
    pipeline.registry.register(FileSimpleTextOutPathCapability("out/test"))

    pipeline.registry.register(FileHtmlOutputProvider("out/test"))
    pipeline.registry.register(FileHtmlStringProvider("out/test"))
    pipeline.registry.register(FileHtmlTextOutPathCapability("out/test"))

    pipeline.registerDocumentRepresentationProducer(TikaTxtDocumentRepresentationProducer(pipeline.registry))
    pipeline.registerDocumentRepresentationProducer(TikaHtmlDocumentRepresentationProducer(pipeline.registry))

    pipeline.registerChunkProducer("sentenceProducer", StanfordNlpSentenceChunkProducer(pipeline.registry))


    return pipeline
}
