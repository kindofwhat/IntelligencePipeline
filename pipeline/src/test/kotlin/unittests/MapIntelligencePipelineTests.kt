package unittests

import datatypes.DataRecord
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.stringify
import org.junit.Ignore
import participants.*
import participants.file.*
import pipeline.IIntelligencePipeline
import pipeline.impl.MapIntelligencePipeline
import java.io.File

@ImplicitReflectionSerializer
class MapIntelligencePipelineTests {

    val baseDir = File(".").absolutePath

    //ignore it for now, maybe find a solution later
    @Ignore
     fun testSimple() {
        runBlocking {
            println(baseDir)
            val pipeline = createPipeline( listOf(DirectoryIngestor("$baseDir/pipeline/src/test/resources/testresources")))
            pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))
            pipeline.run()

            val all = mutableListOf<DataRecord>()


            var i = 0

            withTimeout(10000){
                val dataRecords = pipeline.dataRecords("")
                while(i++<4) {
                    all.add(dataRecords.receive())
                }
            }

            assert(4 == all.size)

            assert (3 == all.filter { dataRecord ->  dataRecord.meta.any { metadata ->  metadata.createdBy == TikaMetadataProducer(pipeline.registry).name}}.size)
            println(all)

        }



    }

    fun createPipeline(ingestors: List<PipelineIngestor>): IIntelligencePipeline {
        val pipeline = MapIntelligencePipeline()
        ingestors.forEach { ingestor -> pipeline.registerIngestor(ingestor)}
        pipeline.registerSideEffect("filewriter") { key, value ->
            fileRepresentationStrategy("out/test",value,"json", true)?.bufferedWriter().use { out -> out?.write(JSON(indented = true).stringify(value)) }
        }

        pipeline.registry.register(FileOriginalContentCapability())

        pipeline.registry.register(FileTxtOutputProvider("$baseDir/out/test"))
        pipeline.registry.register(FileTxtStringProvider("$baseDir/out/test"))
        pipeline.registry.register(FileSimpleTextOutPathCapability("$baseDir/out/test"))

        pipeline.registry.register(FileHtmlOutputProvider("$baseDir/out/test"))
        pipeline.registry.register(FileHtmlStringProvider("$baseDir/out/test"))
        pipeline.registry.register(FileHtmlTextOutPathCapability("$baseDir/out/test"))

        pipeline.registerDocumentRepresentationProducer(TikaTxtDocumentRepresentationProducer(pipeline.registry))
        pipeline.registerDocumentRepresentationProducer(TikaHtmlDocumentRepresentationProducer(pipeline.registry))

        pipeline.registerChunkProducer("sentenceProducer", StanfordNlpSentenceChunkProducer(pipeline.registry))


        return pipeline
    }

}