package unittests

import datatypes.DataRecord
import datatypes.DocumentRepresentation
import org.junit.Ignore
import org.junit.Test
import participants.AzureCognitiveServicesMetadataProducer
import participants.StanfordNlpParserProducer
import participants.TikaMetadataProducer
import participants.file.*
import pipeline.capabilities.DefaultCapabilityRegistry

class MetadataProducerTests {
    val registry= DefaultCapabilityRegistry()

    val pathIn = "src/test/resources"
    val pathOut = "out"

    val htmlIn = FileHtmlStringProvider(pathOut)
    val textIn = FileTxtStringProvider(pathOut)

    init {
        registry.register(FileOriginalContentCapability())
        registry.register(FileTxtOutputProvider(pathOut))
        registry.register(FileHtmlOutputProvider(pathOut))
        registry.register(htmlIn)
        registry.register(textIn)
    }

    @Test
    fun testTika() {
        val handler = TikaMetadataProducer(registry)
        val dataRecord = DataRecord(name = "testDataRecord",
                representation = DocumentRepresentation("$pathIn/test3.docx"))
//                representation = DocumentRepresentation("$pathIn/Speech and Language Processing.pdf"))
        val metadata = handler.metadataFor(dataRecord)
        println(metadata)
        assert(metadata.values.size>0)
        assert(textIn.execute("", dataRecord) != null)
        assert(htmlIn.execute("", dataRecord) != null)
    }
    @Test
    fun testStanfordNlp() {
        val handler = StanfordNlpParserProducer(registry)
        val dataRecord = DataRecord(name = "testDataRecord",
                representation = DocumentRepresentation("$pathIn/test3.docx"))
//                representation = DocumentRepresentation("$pathIn/Speech and Language Processing.pdf"))
        val metadata = handler.metadataFor(dataRecord)
        println(metadata)
        assert(metadata.values.size>0)
        assert(textIn.execute("", dataRecord) != null)
        assert(htmlIn.execute("", dataRecord) != null)
    }

    @Ignore
    fun testAzure() {
        //set the api key here
        val handler = AzureCognitiveServicesMetadataProducer("https://westus.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases", "", registry)
        val dataRecord = DataRecord(name = "testDataRecord",
//                representation = DocumentRepresentation("$pathIn/test3.docx"))
                representation = DocumentRepresentation("$pathIn/Speech and Language Processing.pdf"))
        val metadata = handler.metadataFor(dataRecord)
        println(metadata)
        assert(metadata.values.size>0)
        assert(textIn.execute("", dataRecord) != null)
        assert(htmlIn.execute("", dataRecord) != null)
    }
}