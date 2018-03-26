package participants

import com.github.kittinunf.fuel.httpPost
import datatypes.DataRecord
import datatypes.DocumentRepresentation
import datatypes.Metadata
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations
import edu.stanford.nlp.trees.TreeCoreAnnotations
import kotlinx.coroutines.experimental.channels.SendChannel
import kotlinx.io.InputStream
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JSON
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang.StringUtils
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.sax.ToHTMLContentHandler
import org.apache.tika.sax.ToTextContentHandler
import pipeline.capabilities.*
import java.io.File
import java.util.*
import org.apache.tika.language.LanguageIdentifier
import util.log


class HashMetadataProducer() : MetadataProducer {
    override val name = "hash"

    override fun metadataFor(record: DataRecord): Metadata {
        val file = File(record.representation.path)
        if (file.isFile && file.canRead()) {
            val dig = DigestUtils.shaHex(file.inputStream())
            return Metadata(mapOf("digest" to dig), name)
        }
        return Metadata()
    }
}

@RequiresCapabilities(simpleText)
class StanfordNlpParserProducer(val lookup: CapabilityLookupStrategy) : CapabilityLookupStrategyMetadataProducer<String>(lookup) {
    override val name = "stanford-nlp"
    val props = Properties()
    init {
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref")
    }
    val pipeline = StanfordCoreNLP(props)

    override fun metadataFor(record: DataRecord): Metadata {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        val metadata = mutableMapOf<String, String>()
        val text = lookup.lookup(simpleText, record, String::class.java)
        //val text:String? = record.meta.firstOrNull { metadata -> metadata.createdBy == TikaMetadataProducer().name }?.values?.get("text")
        if(StringUtils.isNotEmpty(text)) {

            val document = Annotation(text)

            // run all Annotators on this text
            pipeline.annotate(document)

            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            val sentences = document.get(CoreAnnotations.SentencesAnnotation::class.java)
            var i=0
            for (sentence in sentences) {
                metadata.put("sentence_" + i++, sentence.toString())
                // traversing the words in the current sentence
                // a CoreLabel is a CoreMap with additional token-specific methods
                var j=0
                for (token in sentence.get(CoreAnnotations.TokensAnnotation::class.java)) {
                    metadata.put("word_${i}_${j++}",token.get(CoreAnnotations.TextAnnotation::class.java))
                    metadata.put("pos_${i}_${j}",token.get(CoreAnnotations.PartOfSpeechAnnotation::class.java))
                    metadata.put("ne_${i}_${j}",token.get(CoreAnnotations.NamedEntityTagAnnotation::class.java))
                }

                // this is the parse tree of the current sentence
                val tree = sentence.get(TreeCoreAnnotations.TreeAnnotation::class.java)
                metadata.put("tree_$i", tree.toString())

                // this is the Stanford dependency graph of the current sentence
                val dependencies = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation::class.java)
                metadata.put("dependencies_$i", dependencies.toCompactString(true))
            }
            return Metadata(values=metadata,createdBy = name)

        }
        // create an empty Annotation just with the given text

        return Metadata()
    }

}

class FileInputStreamProvider:BinaryCapability {
    override fun retrieve(name: String, dataRecord: DataRecord): InputStream? {
        val file = File(dataRecord.representation.path)
        if (file.isFile && file.canRead()) {
            return file.inputStream()
        }
        return null
    }
}

@RequiresCapabilities(originalFileContent)
class TikaMetadataProducer(val lookup: CapabilityLookupStrategy) : MetadataProducer, SimpleTextCapability, HtmlTextCapability,LanguageDetectionCapability {

    override fun  retrieve(name:String, dataRecord: DataRecord): String {
        val metadata=metadataFor(dataRecord)
        var result = ""
        when(name) {
            simpleText -> result = metadata.values.get("text")?:""
            htmlText -> result = metadata.values.get("html")?:""
            languageDetection -> result = metadata.values.get("lang")?:""
        }
        return result
    }

    override val name = "tika-metadata"

    override fun metadataFor(record: DataRecord): Metadata {
        val parser = AutoDetectParser()
        val contentHandler = ToTextContentHandler()
        val htmlContentHandler = ToHTMLContentHandler()
        val metadata = org.apache.tika.metadata.Metadata()


        var inputStream = lookup.lookup(originalFileContent, record, InputStream::class.java)

        //val file = File(record.representation.path)
        if (inputStream != null) {
            val metadataPipeline = mutableMapOf<String, String>()
            parser.parse(inputStream, contentHandler, metadata)

            metadataPipeline.put("text", contentHandler.toString())
            /*
            val ld = LanguageDetector.getDefaultLanguageDetector()
            ld.addText(metadata.get("text"))
            if(ld.hasEnoughText()) {
                metadataPipeline.set("lang", ld.detect().language)

            }*/

            val language = LanguageIdentifier(metadataPipeline.get("text")).language
            metadata.set("lang", language)
            metadata.names().forEach { name ->
                //TODO: multivalued
                metadataPipeline.put(name, metadata.get(name))
            }
            //have to parse it again (!)

            inputStream = lookup.lookup(originalFileContent, record, InputStream::class.java)
            parser.parse(inputStream, htmlContentHandler, metadata)
            metadataPipeline.put("html", htmlContentHandler.toString())
            return Metadata(metadataPipeline, name)
        } else {
            log("No inputstream found for " + record)
            return Metadata()
        }
    }
}

@RequiresCapabilities(simpleText, languageDetection)
class AzureCognitiveServicesMetadataProducer(val host:String, val apiKey:String,val lookup: CapabilityLookupStrategy):CapabilityLookupStrategyMetadataProducer<String>(lookup) {
    override val name = "azure_cognitive_service"

    val allowedLanguages = arrayOf("da","de","en","es","fi","fr","it","ja","ko","nl","no","pl","pt-BR","pt-PT","ru","sv")

    @Serializable
    data class RequestDocument(val id: String, val language:String, val text: String)

    @Serializable
    data class RequestDocuments(val documents: List<RequestDocument>)

    @Serializable
    data class ResponseDocument(val id: String, val keyPhrases: List<String> = arrayListOf())

    @Serializable
    data class ResponseDocuments(val documents:List<ResponseDocument>, val errors: List<String> = arrayListOf())


    override fun metadataFor(record: DataRecord): Metadata {
        val text:String? =lookup.lookup(simpleText,record,String::class.java)
        if(StringUtils.isNotEmpty( text)) {
            var language = lookup.lookup(languageDetection,record,String::class.java)
            if(language==null || !allowedLanguages.contains(language)) {
                language = "de"
            }
            val req = RequestDocuments (listOf(RequestDocument("1", language, text?:"")))

            val requestText = JSON.stringify(req)
            val encoded_text = requestText.toByteArray(charset("UTF-8"))
            val url = host
            val resString = url.httpPost()
                    .header(hashMapOf( "Content-Type" to "text/json", "Ocp-Apim-Subscription-Key" to apiKey))
                    .body(encoded_text)
                    .responseString(Charsets.UTF_8).third.component1()?:""
            val response = JSON.parse<ResponseDocuments>(resString)
            val keyPhrases = mutableMapOf<String,String>()
            response.documents.first().keyPhrases.forEachIndexed { idx, keyPhrase ->
                keyPhrases.put("keyPhrase_$idx" , keyPhrase )
            }
            return Metadata(keyPhrases, name)

        }
        return Metadata()
    }
}

class DirectoryIngestor(val directory: String) : PipelineIngestor {
    override val name = "directory"
    suspend override fun ingest(channel: SendChannel<DocumentRepresentation>) {
        File(directory).walkTopDown().forEach {
            channel.send(DocumentRepresentation(it.absolutePath, this.name))
        }
    }
}

class NoOpIngestor() : PipelineIngestor {
    override val name = "noop"

    suspend override fun ingest(channel: SendChannel<DocumentRepresentation>) {

    }
}
