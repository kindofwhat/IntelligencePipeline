package integrationtests

import datatypes.DataRecord
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Ignore
import org.junit.Test
import orientdb.OrientDBPipeline
import participants.*
import participants.file.*
import java.io.File
import java.lang.Thread.sleep

class OrientDBPipelineTest {

    companion object {
        val baseDir = File(".").absolutePath

        fun createPipeline(name: String, ingestors: List<PipelineIngestor>, producers: List<MetadataProducer>): OrientDBPipeline {
            val pipeline = orientdb.OrientDBPipeline("remote:localhost", "ip", "root", "root")
//            val pipeline = OrientDBPipeline("memory:", name, "admin", "admin")
            ingestors.forEach { pipeline.registerIngestor(it) }
            producers.forEach { pipeline.registerMetadataProducer(it) }
            val testDir = "$baseDir/out/test"


            pipeline.registry.register(FileOriginalContentCapability())

            pipeline.registry.register(FileTxtOutputProvider(testDir))
            pipeline.registry.register(FileTxtStringProvider(testDir))
            pipeline.registry.register(FileSimpleTextOutPathCapability(testDir))

            pipeline.registry.register(FileHtmlOutputProvider(testDir))
            pipeline.registry.register(FileHtmlStringProvider(testDir))
            pipeline.registry.register(FileHtmlTextOutPathCapability(testDir))

            pipeline.registerDocumentRepresentationProducer(TikaTxtDocumentRepresentationProducer(pipeline.registry))
            pipeline.registerDocumentRepresentationProducer(TikaHtmlDocumentRepresentationProducer(pipeline.registry))

            pipeline.registerChunkProducer("sentenceProducer", StanfordNlpSentenceChunkProducer(pipeline.registry))

            return pipeline
        }


        fun runPipeline(pipeline: OrientDBPipeline,
                        predicate: (datatypes.DataRecord) -> Boolean,
                        expectedResults: Int,
                        timeout: Long = 60000L,
                        id: String = ""): List<datatypes.DataRecord> {
            val view = mutableListOf<DataRecord>()
            runBlocking {
                val job = GlobalScope.launch {
                    pipeline.run()
                }
                job.join()
                sleep(4000)
                withTimeout(timeout) {
                    var i = 0
                    while(i<expectedResults) {
                        val dataRecords = pipeline.dataRecords(id)
                        val record = dataRecords.receive()
                        if(predicate(record)) {
                            view.add(record)
                            i++
                        }
                        i=expectedResults
                    }
                }
                pipeline.stop()
//            view = createDataRecords(storeName)
            }
            return view
        }
    }

    @Test
    @Throws(Exception::class)
    fun testHashMetadataProducer() {
        val name = "testHashMetadataProducer"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("../pipeline-spi/src/test/resources/testresources")), emptyList())

        pipeline.registerMetadataProducer(HashMetadataProducer())

        val view = runPipeline(pipeline, { kv: datatypes.DataRecord ->
            kv.meta.any { metadata -> metadata.createdBy == HashMetadataProducer().name }
        }, 3)
        pipeline.stop()
    }


    @Test
    @Throws(Exception::class)
    fun testRogueMetadataProducer() {
        println("baseDir is $baseDir")

        class RogueMetadataProducer() : MetadataProducer {
            override val name = "rogue";
            override fun metadataFor(record: datatypes.DataRecord): datatypes.Metadata {
                throw RuntimeException("na, won't to!")
            }
        }

        val name = "testRogueMetadataProducer"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("../pipeline-spi/src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))
        pipeline.registerMetadataProducer(RogueMetadataProducer())

        val view = runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == TikaMetadataProducer(pipeline.registry).name } }, 3)
        pipeline.stop()
    }

    @Ignore
    @Throws(Exception::class)
    fun testLargerDirectory() {
        val name = "testLargeDirectory"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("c:\\Users\\Christian\\Documents\\Architecture")), emptyList())

        pipeline.registerMetadataProducer(HashMetadataProducer())
        val tikaMetadataProducer = TikaMetadataProducer(pipeline.registry)
        pipeline.registerMetadataProducer(tikaMetadataProducer)


        runPipeline(pipeline, { record: datatypes.DataRecord -> true }, 2000)
        pipeline.stop()
    }



    @Test
    @Throws(Exception::class)
    fun testStanfordNlpParser() {
        val name = "testStanfordNlpParser"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("../pipeline-spi/src/test/resources/testresources")), emptyList<MetadataProducer>())

        val nlpParserProducer = StanfordNlpParserProducer(pipeline.registry)
        val tikaMetadataProducer = TikaMetadataProducer(pipeline.registry)
        pipeline.registerMetadataProducer(nlpParserProducer)
        pipeline.registerMetadataProducer(tikaMetadataProducer)

        val view = runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == nlpParserProducer.name } }, 3)
        pipeline.stop()
    }



    @Test
    fun testDirectoryCrawlAndTika() {
        val name = "testDirectoryCrawlAndTika"
        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("../pipeline-spi/src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerMetadataProducer(TikaMetadataProducer(pipeline.registry))
        runPipeline(pipeline, { kv -> kv.meta.any { metadata -> metadata.createdBy == TikaMetadataProducer(pipeline.registry).name } }, 3)
        pipeline.stop()
    }



}