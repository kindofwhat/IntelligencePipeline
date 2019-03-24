import kotlinx.serialization.ImplicitReflectionSerializer
import participants.*
import participants.file.*
import pipeline.IIntelligencePipeline
import pipeline.impl.KafkaIntelligencePipeline


@ImplicitReflectionSerializer
fun createPipeline(hostUrl:String, stateDir:String,
                   ingestors: List<PipelineIngestor> = emptyList(),
                   producers:List<MetadataProducer> = emptyList()): IIntelligencePipeline {
    val pipeline = KafkaIntelligencePipeline(hostUrl, stateDir, "testPipeline1")
    //val pipeline = ChannelIntelligencePipeline()
    ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor) }
    producers.forEach { producer -> pipeline.registerMetadataProducer(producer) }
    pipeline.registerSideEffect("printer", { key, value -> println(">>>$key: $value<<<") })

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

