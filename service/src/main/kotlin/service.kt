import datatypes.DocumentRepresentation
import commands.StartPipeline
import io.javalin.Javalin
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.parse
import kotlinx.serialization.stringify
import participants.*
import participants.file.*
import pipeline.impl.KafkaIntelligencePipeline

@ImplicitReflectionSerializer
var  pipeline: KafkaIntelligencePipeline?  =null //= KafkaIntelligencePipeline()

@ImplicitReflectionSerializer
fun main(args: Array<String>) {

    val app =   Javalin.create().apply {
        port(7000)
        exception(Exception::class.java) { e, ctx -> e.printStackTrace() }
        error(404) { ctx -> ctx.json("not found") }
        enableCorsForAllOrigins()
    }.start()

    with(app) {

        get("/") { ctx -> ctx.result("Hello World7") }
        get("/test") { ctx -> ctx.result(JSON.stringify(DocumentRepresentation("path", "test"))) }
        post("/startPipeline") { ctx ->
            startPipeline( JSON.parse(ctx.body()?:"") as StartPipeline, app)
        }
        post("/stopPipeline") { ctx ->
            stopPipeline()
        }
    }


}

@ImplicitReflectionSerializer
fun stopPipeline() {
    pipeline?.stop()
}

@ImplicitReflectionSerializer
fun startPipeline(command: StartPipeline, app:Javalin) {
    pipeline = createPipeline(command.bootstrap, command.stateDirectory,
            listOf(DirectoryIngestor(command.scanDirectory)), emptyList(),app)
    pipeline?.run()
}


@ImplicitReflectionSerializer
fun createPipeline(hostUrl:String, stateDir:String, ingestors: List<PipelineIngestor>, producers:List<MetadataProducer>, app:Javalin): KafkaIntelligencePipeline {
    val pipeline = KafkaIntelligencePipeline(hostUrl, stateDir, "testPipeline1")
    ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor)}
    producers.forEach { producer -> pipeline.registerMetadataProducer(producer)}
//    pipeline.registerSideEffect("printer", {key, value -> println("$key: $value")  } )

    pipeline.registerSideEffect("websocket-datarecord", { key, value ->
        val ws = app.ws("/websocket/datarecord") { ws ->
            ws.onConnect { session ->
                println("Connected to datarecord websocket")
                session.remote.sendString( JSON.stringify(value))
            }
            ws.onClose { session, statusCode, reason -> println("Closed") }
            ws.onError { session, throwable -> println("Errored $throwable") }
            ws.onMessage { session, msg -> session.remote.sendString( JSON.stringify(value))}
        }
    })
/*
    pipeline.registerSideEffect("filewriter", {key, value ->
        fileRepresentationStrategy("out/test",value,"json", true)?.bufferedWriter().use { out -> out?.write(JSON(indented = true).stringify(value)) }
    } )
*/
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
