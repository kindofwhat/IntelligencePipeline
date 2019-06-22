import kotlinx.serialization.ImplicitReflectionSerializer
import participants.*
import participants.file.*
import pipeline.IIntelligencePipeline
import orientdb.OrientDBPipeline
import java.util.*


@ImplicitReflectionSerializer
fun createPipeline(docDir:String,outDir:String, hostUrl:String, dbName:String, user:String, password:String,
                   ingestors: List<PipelineIngestor> = emptyList(),
                   producers:List<MetadataProducer> = emptyList()): OrientDBPipeline {
    val pipeline = OrientDBPipeline(hostUrl, dbName,  user, password)
    //val pipelineActions = MapIntelligencePipeline()
    ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor) }
    producers.forEach { producer -> pipeline.registerMetadataProducer(producer) }
   // pipelineActions.registerSideEffect("printer", { key, value -> println(">>>$key: $value<<<") })

    pipeline.registry.register(FileOriginalContentCapability())

    pipeline.registry.register(FileTxtOutputProvider(outDir))
    pipeline.registry.register(FileTxtStringProvider(outDir))
    pipeline.registry.register(FileSimpleTextOutPathCapability(outDir))

    pipeline.registry.register(FileHtmlOutputProvider(outDir))
    pipeline.registry.register(FileHtmlStringProvider(outDir))
    pipeline.registry.register(FileHtmlTextOutPathCapability(outDir))

    pipeline.registerDocumentRepresentationProducer(TikaTxtDocumentRepresentationProducer(pipeline.registry))
    pipeline.registerDocumentRepresentationProducer(TikaHtmlDocumentRepresentationProducer(pipeline.registry))

    pipeline.registerChunkProducer("sentenceProducer", StanfordNlpSentenceChunkProducer(pipeline.registry))
    pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))
    pipeline.registerChunkNamedEntityExtractor(StanfordNEExtractor())


    return pipeline
}

