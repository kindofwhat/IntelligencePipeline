import datatypes.DocumentRepresentation
import io.javalin.Javalin
import kotlinx.serialization.json.JSON
import participants.*
import participants.file.*
import pipeline.IntelligencePipeline

fun main(args: Array<String>) {
    val pipeline: IntelligencePipeline //= IntelligencePipeline()

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
            startPipeline(ctx.param("bootstrap"),"tmp")
        }
        post("/stopPipeline") { ctx ->
            stopPipeline()
        }
    }


}

fun stopPipeline() {

    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
}

fun startPipeline(boostrap: String?, dir: String) {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
}


fun createPipeline(hostUrl:String, stateDir:String, ingestors: List<PipelineIngestor>, producers:List<MetadataProducer>): IntelligencePipeline {
    val pipeline = IntelligencePipeline(hostUrl, stateDir,"testPipeline")
    ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor)}
    producers.forEach { producer -> pipeline.registerMetadataProducer(producer)}
//            pipeline.registerSideEffect("printer", {key, value -> println("$key: $value")  } )
    pipeline.registerSideEffect("filewriter", {key, value ->
        fileRepresentationStrategy("out/test",value,"json", true)?.bufferedWriter().use { out -> out?.write(JSON(indented = true).stringify(value)) }
    } )

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
