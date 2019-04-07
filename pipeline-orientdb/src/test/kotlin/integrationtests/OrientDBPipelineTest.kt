package integrationtests

import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import datatypes.DataRecord
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.filter
import kotlinx.coroutines.channels.take
import kotlinx.coroutines.channels.toList
import org.junit.Before
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
        val connection = "remote:localhost"
        val db = "ip"
        val user = "root"
        val password = "root"

        fun createPipeline(name: String, ingestors: List<PipelineIngestor>, producers: List<MetadataProducer>): OrientDBPipeline {
            val pipeline = orientdb.OrientDBPipeline(connection, db, user, password)
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

        @Before
        private fun clearTables() {
            val config = OrientDBConfig.defaultConfig()
            val orient = OrientDB(connection, user, password, config)
            val db = orient.open(db, user, password)
            db.command("DELETE FROM DataRecord")
            db.command("DELETE FROM DocumentRepresentation")
            db.command("DELETE FROM Chunk")
            db.commit()


        }

        fun runPipeline(pipeline: OrientDBPipeline,
                        predicate: (datatypes.DataRecord) -> Boolean,
                        expectedResults: Int,
                        timeout: Long = 40000L,
                        id: String = ""): List<datatypes.DataRecord> {
            var view = listOf<DataRecord>()
            clearTables()
            runBlocking {
                val job = GlobalScope.launch {
                    pipeline.run()
                }
                job.join()
                // sleep(timeout)
                withTimeout(timeout) {
                    var ok = false;
                    while (!ok) {
                        try {
                            withTimeout(500) {
                                view = pipeline.dataRecords(id)
                                        .filter { predicate(it) }.take(expectedResults).toList()
                                println("ok: ${view.size}")
                                ok = true
                            }

                        } catch (e: TimeoutCancellationException) {
                            println("timeout, retrying")
                            sleep(500)
                        }
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
            override suspend fun produce(record: datatypes.DataRecord): datatypes.Metadata {
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

    @Test
    @Throws(Exception::class)
    fun testLargerDirectory() {
        val name = "testLargeDirectory"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("/home/christian/Dokumente")), emptyList())

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
//        val view = runPipeline(pipeline, { kv -> true }, 3)
        view.map { dataRecord -> dataRecord.meta }
                .map { metas -> metas.find { metadata -> metadata.createdBy == nlpParserProducer.name }}
                .forEach { meta-> println(meta)}
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