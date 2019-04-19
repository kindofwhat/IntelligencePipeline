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

class OrientDBPipelineTest {


    companion object {
        val baseDir = File(".").absolutePath
        val connection = "remote:localhost"
        val db = "ip"
        val user = "root"
        val password = "root"
        val config = OrientDBConfig.defaultConfig()
        val orient = OrientDB(connection, user, password, config)

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

            //  pipeline.registerChunkProducer("sentenceProducer", StanfordNlpSentenceChunkProducer(pipeline.registry))

            return pipeline
        }
    }

    @Before
    fun clearTables() {
        if(orient.exists(db)) {
            val session = orient.open(db, user, password)
            session.command("DELETE FROM DataRecord")
            session.command("DELETE FROM Metadata")
            session.command("DELETE FROM DocumentRepresentation")
            session.command("DELETE FROM Chunk")
            session.commit()
        }


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



        runBlocking {
            val job = GlobalScope.launch {
                pipeline.run()
            }
            job.join()
            // sleep(timeout)
            withTimeout(10_000) {
                delay(2000)
                var res = 0L
                while (res < 3) {
                    val session = orient.open(db, user, password)
                    res = session.query("SELECT FROM Metadata where createdBy = '${nlpParserProducer.name}'").stream().count()
                    session.close()
                }

            }
            pipeline.stop()
//            view = createDataRecords(storeName)
        }


    }

}