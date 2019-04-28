package integrationtests

import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import kotlinx.coroutines.*
import org.junit.Before
import org.junit.Test
import orientdb.OrientDBPipeline
import participants.*
import participants.file.*
import java.io.File
import java.net.URI

class OrientDBPipelineTest {


    companion object {
        val baseDir = URI(File(".").absolutePath).normalize().toString()
        val connection = "memory:"
        val db = "ip"
        val user = "admin"
        val password = "admin"


        fun createPipeline(name: String, ingestors: List<PipelineIngestor>, producers: List<MetadataProducer>): OrientDBPipeline {
            val pipeline = orientdb.OrientDBPipeline(connection, db, user, password)
//            val pipeline = OrientDBPipeline("memory:", name, "admin", "admin")
            ingestors.forEach { pipeline.registerIngestor(it) }
            producers.forEach { pipeline.registerMetadataProducer(it) }
            println("working in $baseDir")
            val testDir = "${baseDir}out/test"


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
            pipeline.registerChunkMetadataProducer(TikaChunkLanguageDetection())
            return pipeline
        }
    }


    private fun queryTest(pipeline: OrientDBPipeline, query: String, expectedResults: Int, maxWait: Long = 30_000) {
        runBlocking {
            val job = GlobalScope.launch {
                pipeline.run()
            }
            job.join()
            // sleep(timeout)
            withTimeout(maxWait) {
                delay(2000)
                pipeline.korient.withSession { session ->
                    var res = 0L
                    while (res < expectedResults) {
                        res = session.query(query).stream().count()
                    }

                }

            }
            pipeline.stop()
            //            view = createDataRecords(storeName)
        }
    }


    @Test
    @Throws(Exception::class)
    fun testStanfordNEExtractor() {
        val name = "testStanfordNEExtractor"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("${baseDir}pipeline-spi/src/test/resources/testresources")), emptyList<MetadataProducer>())

        pipeline.registerChunkNamedEntityExtractor(StanfordNEExtractor())


        queryTest(pipeline, "SELECT FROM NamedEntity where type = 'PERSON' and value ='Paris Hilton'", 1)
    }


    @Test
    @Throws(Exception::class)
    fun testStanfordNlpParser() {
        val name = "testStanfordNlpParser"

        val pipeline = createPipeline(name,
                listOf(DirectoryIngestor("${baseDir}pipeline-spi/src/test/resources/testresources")), emptyList<MetadataProducer>())
//                listOf(DirectoryIngestor("/home/christian/Dokumente")), emptyList<MetadataProducer>())

        val nlpParserProducer = StanfordNlpParserProducer(pipeline.registry)
        val tikaMetadataProducer = TikaMetadataProducer(pipeline.registry)
        pipeline.registerMetadataProducer(nlpParserProducer)
        pipeline.registerMetadataProducer(tikaMetadataProducer)
        pipeline.registerChunkNamedEntityExtractor(StanfordNEExtractor())


        queryTest(pipeline, "SELECT FROM Metadata where createdBy = '${nlpParserProducer.name}'", 3)
        queryTest(pipeline, "SELECT FROM Metadata where createdBy = '${tikaMetadataProducer.name}'", 3)
    }


}