package unittests

import datatypes.DataRecord
import datatypes.DocumentRepresentation
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Ignore
import org.junit.Test
import participants.*
import participants.file.*
import pipeline.capabilities.DefaultCapabilityRegistry

class ChunkProducerTests {
    val registry= DefaultCapabilityRegistry()

    val pathIn = "src/test/resources/testresources"
    val pathOut = "out"

    init {
        registry.register(FileOriginalContentCapability())

        registry.register(FileTxtOutputProvider(pathOut))
        registry.register(FileSimpleTextOutPathCapability(pathOut))
        registry.register(FileTxtStringProvider(pathOut))

        registry.register(FileHtmlOutputProvider(pathOut))
        registry.register(FileHtmlTextOutPathCapability(pathOut))
        registry.register(FileHtmlStringProvider(pathOut))

    }

    @Test
    fun testStanfordSentenceProducer() {
        val dataRecord = datatypes.DataRecord(name = "testDataRecord",
                representation = datatypes.DocumentRepresentation("$pathIn/test3.docx"))
//                representation = DocumentRepresentation("$pathIn/../Speech and Language Processing.pdf"))


        val output = TikaTxtDocumentRepresentationProducer(registry)
        output.documentRepresentationFor(dataRecord)

        val producer = StanfordNlpSentenceChunkProducer(registry)

        val chunks = runBlocking {  producer.chunks(dataRecord,1)}

        chunks.forEach { println(it) }
        assert(chunks.toList().size>0)
    }


}