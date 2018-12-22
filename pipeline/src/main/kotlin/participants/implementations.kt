package participants

import com.github.kittinunf.fuel.httpPost
import com.google.cloud.language.v1.AnalyzeEntitiesRequest
import com.google.cloud.language.v1.Document
import com.google.cloud.language.v1.EncodingType
import datatypes.DataRecord
import datatypes.Metadata
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations
import edu.stanford.nlp.trees.TreeCoreAnnotations
import facts.Proposer
import facts.Proposition
import facts.PropositionType
import kotlinx.coroutines.channels.SendChannel
import kotlinx.io.InputStream
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JSON
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang.StringUtils
import org.apache.tika.io.NullOutputStream
import org.apache.tika.language.LanguageIdentifier
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.sax.ToHTMLContentHandler
import org.apache.tika.sax.ToTextContentHandler
import org.xml.sax.ContentHandler
import org.xml.sax.helpers.DefaultHandler
import pipeline.capabilities.*
import java.io.File
import java.io.OutputStream
import java.util.*
import kotlinx.coroutines.*
import com.google.cloud.language.v1.LanguageServiceClient
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.parse
import kotlinx.serialization.stringify


class HashMetadataProducer() : MetadataProducer {
    override val name = "hash"

    override fun metadataFor(record: datatypes.DataRecord): datatypes.Metadata {
        val file = File(record.representation.path)
        if (file.isFile && file.canRead()) {
            val dig = DigestUtils.shaHex(file.inputStream())
            return datatypes.Metadata(mapOf("digest" to dig), name)
        }
        return datatypes.Metadata()
    }
}



@RequiresCapabilities(simpleTextIn)
class StanfordNlpSentenceChunkProducer(val lookup: CapabilityLookupStrategy):ChunkProducer {
    val props = Properties()
    override val name: String="sentenceChunkProducer"
    init {
//        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref")
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner")
    }
    val pipeline = StanfordCoreNLP(props)

    override suspend fun chunks(record: datatypes.DataRecord, recordId:Long): Sequence<datatypes.Chunk> {
        val text = lookup.lookup(simpleTextIn, record, String::class.java)
        //val text:String? = record.meta.firstOrNull { metadata -> metadata.createdBy == TikaMetadataProducer().name }?.values?.get("text")
        if (StringUtils.isNotEmpty(text)) {
            return sequence {
                val document = Annotation(text)

                // run all Annotators on this text
                pipeline.annotate(document)

                // these are all the sentences in this document
                // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
                val sentences = document.get(CoreAnnotations.SentencesAnnotation::class.java)
                yield(datatypes.Chunk(type = datatypes.ChunkType.SENTENCE, command = datatypes.ChunkCommand.START, index = 0L, parentId = recordId))
                var idx:Long=0
                for (sentence in sentences) {
                    yield(datatypes.Chunk(type = datatypes.ChunkType.SENTENCE, content = sentence.toString(), index = idx++, parentId = recordId))
                }
                yield(datatypes.Chunk(type = datatypes.ChunkType.SENTENCE, command = datatypes.ChunkCommand.LAST, index = 0L, parentId = recordId))
            }
        }
        return emptySequence()
    }
}

@RequiresCapabilities(simpleTextIn)
class StanfordNlpParserProducer(val lookup: CapabilityLookupStrategy) : CapabilityLookupStrategyMetadataProducer<String>(lookup) {
    override val name = "stanford-nlp"
    val props = Properties()
    init {
//        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref")
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner")
    }
    val pipeline = StanfordCoreNLP(props)

    override fun metadataFor(record: datatypes.DataRecord): datatypes.Metadata {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        val metadata = mutableMapOf<String, String>()
        val text = lookup.lookup(simpleTextIn, record, String::class.java)
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
                if(tree != null)
                    metadata.put("tree_$i", tree.toString())

                // this is the Stanford dependency graph of the current sentence
                val dependencies = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation::class.java)
                if(dependencies != null)
                    metadata.put("dependencies_$i", dependencies.toCompactString(true))
            }
            return datatypes.Metadata(values = metadata, createdBy = name)

        }
        // create an empty Annotation just with the given text

        return datatypes.Metadata()
    }

}



fun documentRepresenation(record: datatypes.DataRecord, textOutCapabilityName:String, textOutCapabilityPathName:String,
                          lookup:CapabilityLookupStrategy, name:String,
                          contHandlerCreator: (outputStream: OutputStream) -> ContentHandler): datatypes.DocumentRepresentation {
    val parser = AutoDetectParser()
    var inputStream = lookup.lookup(originalContentIn, record, InputStream::class.java)
    var outputStream =  lookup.lookup(textOutCapabilityName, record, OutputStream::class.java)
    var outPath =  lookup.lookup(textOutCapabilityPathName, record, String::class.java)

    //extract fulltext
    if (inputStream != null && outputStream != null && outPath != null) {
        inputStream.use { input ->
            outputStream.use { out ->
                val contentHandler = contHandlerCreator.invoke(out) //ToTextContentHandler(out, "utf-8")
                val metadata = org.apache.tika.metadata.Metadata()
                parser.parse(input, contentHandler, metadata)
            }
        }
        return datatypes.DocumentRepresentation(outPath, name)
    }
    return datatypes.DocumentRepresentation()
}

@RequiresCapabilities(originalContentIn,htmlTextOut, htmlTextOutPath)
class TikaHtmlDocumentRepresentationProducer(val lookup:CapabilityLookupStrategy, override val name:String = "tika-html"):DocumentRepresentationProducer {
    override fun documentRepresentationFor(record: datatypes.DataRecord): datatypes.DocumentRepresentation {
        return documentRepresenation(record, htmlTextOut, htmlTextOutPath,lookup,"tika-html"
        ) { out ->
            ToHTMLContentHandler(out,"utf-8")}
    }
}

@RequiresCapabilities(originalContentIn, simpleTextOut, simpleTextOutPath)
class TikaTxtDocumentRepresentationProducer(val lookup:CapabilityLookupStrategy, override val name:String = "tika-txt"):DocumentRepresentationProducer {
    override fun documentRepresentationFor(record: datatypes.DataRecord): datatypes.DocumentRepresentation {
        return documentRepresenation(record, simpleTextOut, simpleTextOutPath,lookup,"tika-txt"
        ) { out ->  ToTextContentHandler(out,"utf-8")}
    }
}

@HasCapabilities(languageDetection)
class TikaChunkLanguageDetection() :ChunkMetadataProducer {
    override val name: String="tika-lang-chunk"
    override fun metadataFor(chunk: datatypes.Chunk): datatypes.Metadata {
        if(StringUtils.isNotEmpty(chunk.content)) {
            return datatypes.Metadata(values = mapOf(LanguageIdentifier(chunk.content).language to chunk.content), createdBy = name)
        }
        return datatypes.Metadata()
    }
}

class Language(result:String): PropositionType<String>("language", result)
/**
 * Does 2 things at once:
 * <ul>
 *     <li>language detection</li>
 *     <li>metadata extraction</li>
 *     </ul>
 */
@RequiresCapabilities(originalContentIn)
@HasCapabilities(languageDetection)
class TikaMetadataProducer  (val lookup: CapabilityLookupStrategy) :
        MetadataProducer,
        Capability<String?>,
        Proposer<DataRecord, Language> {

    override fun propose(proposedFor: datatypes.DataRecord): Proposition<Language> {
        val detected = extractLang(proposedFor)
        val confidence =if(detected == null) 0.0f else 1.0f
        return Proposition(Language(result= detected ?: ""), confidence)
    }

    override val name = "tika-metadata"


    override fun  execute(name:String, dataRecord: datatypes.DataRecord): String? {
        return dataRecord.meta.filter { it.createdBy == name }.firstOrNull()?.values?.get("lang")?:extractLang(dataRecord)?:""
    }

    private fun extractLang(record: datatypes.DataRecord): String? {
        val text =  lookup.lookup(simpleTextIn, record, String::class.java)
        if(StringUtils.isNotEmpty(text)) {
            return LanguageIdentifier(text).language
        }
        return null
    }


    override fun metadataFor(record: datatypes.DataRecord): datatypes.Metadata {
        val parser = AutoDetectParser()
        val metadataPipeline = mutableMapOf<String, String>()
        val metadata = org.apache.tika.metadata.Metadata()


        var inputStream = lookup.lookup(originalContentIn, record, InputStream::class.java)
        var outputStream =  NullOutputStream()


        //extract fulltext
        if (inputStream != null && outputStream != null) {
            inputStream.use { input ->
                val contentHandler = DefaultHandler()
                parser.parse(input, contentHandler, metadata)
                metadata.set("lang", extractLang(record)?:"")
            }
        }
        metadata.names().forEach { name ->
            metadataPipeline.put(name,metadata.get(name))
        }
        if(metadataPipeline.size>0)  return datatypes.Metadata(metadataPipeline, name)
        else return datatypes.Metadata()
    }
}

@RequiresCapabilities(simpleTextIn, languageDetection)
class GoogleNLPMetadataProducer(val lookup: CapabilityLookupStrategy):CapabilityLookupStrategyMetadataProducer<String>(lookup) {
    override val name = "google_nlp_metadata"
    override fun metadataFor(record: DataRecord): Metadata {
        val entities = mutableMapOf<String,String>()
        val text:String? =lookup.lookup(simpleTextIn,record,String::class.java)
        if(StringUtils.isNotEmpty( text)) {
            val client = LanguageServiceClient.create()
            val doc = Document.newBuilder()
                    .setContent(text).setType(Document.Type.PLAIN_TEXT).build()
            val analyzeRequest = AnalyzeEntitiesRequest.newBuilder()
                    .setDocument(doc)
                    .setEncodingType(EncodingType.UTF8)
                    .build()
            val analyseResponse = client.analyzeEntities(analyzeRequest)
            analyseResponse.entitiesList.forEach{ entity ->
                entities.put(entity.name,entity.type.name)
            }
        }
        return Metadata(entities)
    }
}

@RequiresCapabilities(simpleTextIn, languageDetection)
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


    @ImplicitReflectionSerializer
    override fun metadataFor(record: datatypes.DataRecord): datatypes.Metadata {
        val text:String? =lookup.lookup(simpleTextIn,record,String::class.java)
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
            return datatypes.Metadata(keyPhrases, name)

        }
        return datatypes.Metadata()
    }
}

class DirectoryIngestor(val directory: String) : PipelineIngestor {
    override val name = "directory"
    suspend override fun ingest(channel: SendChannel<datatypes.DocumentRepresentation>) {
        File(directory).walkTopDown().forEach {
            channel.send(datatypes.DocumentRepresentation(it.absolutePath, this.name))
        }
    }
}
