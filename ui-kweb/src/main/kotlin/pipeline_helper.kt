import kotlinx.serialization.ImplicitReflectionSerializer
import participants.*
import participants.file.*
import pipeline.IIntelligencePipeline
import pipeline.impl.KafkaIntelligencePipeline
import java.util.*


@ImplicitReflectionSerializer
fun createPipeline(hostUrl:String, stateDir:String,
                   ingestors: List<PipelineIngestor> = emptyList(),
                   producers:List<MetadataProducer> = emptyList()): IIntelligencePipeline {
    val pipeline = KafkaIntelligencePipeline(hostUrl, stateDir, "uiPipeline${System.currentTimeMillis()}")
    //val pipelineActions = MapIntelligencePipeline()
    ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor) }
    producers.forEach { producer -> pipeline.registerMetadataProducer(producer) }
   // pipelineActions.registerSideEffect("printer", { key, value -> println(">>>$key: $value<<<") })

    pipeline.registry.register(FileOriginalContentCapability())

    pipeline.registry.register(FileTxtOutputProvider("$stateDir/out2"))
    pipeline.registry.register(FileTxtStringProvider("$stateDir/out2"))
    pipeline.registry.register(FileSimpleTextOutPathCapability("$stateDir/out2"))

    pipeline.registry.register(FileHtmlOutputProvider("$stateDir/out2"))
    pipeline.registry.register(FileHtmlStringProvider("$stateDir/out2"))
    pipeline.registry.register(FileHtmlTextOutPathCapability("$stateDir/out2"))

    pipeline.registerDocumentRepresentationProducer(TikaTxtDocumentRepresentationProducer(pipeline.registry))
    pipeline.registerDocumentRepresentationProducer(TikaHtmlDocumentRepresentationProducer(pipeline.registry))

    pipeline.registerChunkProducer("sentenceProducer", StanfordNlpSentenceChunkProducer(pipeline.registry))
    pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))


    return pipeline
}

