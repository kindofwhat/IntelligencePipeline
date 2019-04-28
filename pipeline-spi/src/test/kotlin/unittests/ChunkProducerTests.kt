package unittests

import kotlinx.coroutines.runBlocking
import org.junit.Test
import participants.*
import participants.file.*
import pipeline.capabilities.DefaultCapabilityRegistry

class ChunkProducerTests {
    val registry= DefaultCapabilityRegistry()

    val pathIn = "src/test/resources/testresources"
    val pathOut = "out/"

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
        runBlocking { output.produce(dataRecord)}

        val producer = StanfordNlpSentenceChunkProducer(registry)

        val chunks = runBlocking {  producer.produce(dataRecord)}

        chunks?.forEach { println(it) }
        assert(chunks != null && chunks.toList().size>0)
    }


}