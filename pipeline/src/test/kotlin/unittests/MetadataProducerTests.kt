package unittests

import kotlinx.coroutines.experimental.runBlocking
import org.junit.Ignore
import org.junit.Test
import participants.AzureCognitiveServicesMetadataProducer
import participants.StanfordNlpParserProducer
import participants.TikaHtmlDocumentRepresentationProducer
import participants.TikaMetadataProducer
import participants.file.*
import pipeline.capabilities.DefaultCapabilityRegistry

class MetadataProducerTests {
    val registry= DefaultCapabilityRegistry()

    val pathIn = "src/test/resources/testresources"
    val pathOut = "out"

    val htmlIn = FileHtmlStringProvider(pathOut)
    val textIn = FileTxtStringProvider(pathOut)

    init {
        registry.register(FileOriginalContentCapability())
        registry.register(FileTxtOutputProvider(pathOut))
        registry.register(FileSimpleTextOutPathCapability(pathOut))
        registry.register(FileHtmlOutputProvider(pathOut))
        registry.register(FileHtmlTextOutPathCapability(pathOut))
        registry.register(htmlIn)
        registry.register(textIn)
    }

    @Test
    fun testTikaDocumentRepresentationProducer() {
        val docrepProd = TikaHtmlDocumentRepresentationProducer(registry)

        val dataRecord = datatypes.DataRecord(name = "testDataRecord",
                representation = datatypes.DocumentRepresentation("$pathIn/test3.docx"))
//                representation = DocumentRepresentation("$pathIn/../Speech and Language Processing.pdf"))
        val docrep= docrepProd.documentRepresentationFor(dataRecord)
        println(docrep)
        assert(docrep.createdBy == docrepProd.name)
    }
    @Test
    fun testTikaMetadataProducer() {
        val handler = TikaMetadataProducer(registry)
        val dataRecord = datatypes.DataRecord(name = "testDataRecord",
                representation = datatypes.DocumentRepresentation("$pathIn/test3.docx"))
//                representation = DocumentRepresentation("$pathIn/Speech and Language Processing.pdf"))
        val metadata = runBlocking { handler.metadataFor(dataRecord) }
        println(metadata)
        assert(metadata.values.size>0)
    }

    @Test
    fun testStanfordNlp() {
        val handler = StanfordNlpParserProducer(registry)
        val dataRecord = datatypes.DataRecord(name = "testDataRecord",
                representation = datatypes.DocumentRepresentation("$pathIn/test3.docx"))
//                representation = DocumentRepresentation("$pathIn/Speech and Language Processing.pdf"))
        val metadata =  runBlocking { handler.metadataFor(dataRecord) }
        println(metadata)
        assert(metadata.values.size>0)
    }

    @Ignore
    fun testAzure() {
        //set the api key here
        val handler = AzureCognitiveServicesMetadataProducer("https://westus.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases", "", registry)
        val dataRecord = datatypes.DataRecord(name = "testDataRecord",
//                representation = DocumentRepresentation("$pathIn/test3.docx"))
                representation = datatypes.DocumentRepresentation("$pathIn/Speech and Language Processing.pdf"))
        val metadata =  runBlocking { handler.metadataFor(dataRecord) }
        println(metadata)
        assert(metadata.values.size>0)
    }
}