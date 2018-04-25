package participants

import com.github.kittinunf.fuel.httpPost
import datatypes.*
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
import kotlin.coroutines.experimental.buildSequence


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



@RequiresCapabilities(simpleTextIn)
class StanfordNlpSentenceChunkProducer(val lookup: CapabilityLookupStrategy):ChunkProducer {
    val props = Properties()
    override val name: String="sentenceChunkProducer"
    init {
//        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref")
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner")
    }
    val pipeline = StanfordCoreNLP(props)

    override suspend fun chunks(record: DataRecord): Sequence<Chunk> {
        val text = lookup.lookup(simpleTextIn, record, String::class.java)
        //val text:String? = record.meta.firstOrNull { metadata -> metadata.createdBy == TikaMetadataProducer().name }?.values?.get("text")
        if (StringUtils.isNotEmpty(text)) {
            return buildSequence {
                val document = Annotation(text)

                // run all Annotators on this text
                pipeline.annotate(document)

                // these are all the sentences in this document
                // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
                val sentences = document.get(CoreAnnotations.SentencesAnnotation::class.java)
                yield(Chunk(type=ChunkType.SENTENCE, command = ChunkCommand.START))
                for (sentence in sentences) {
                    yield(Chunk(type = ChunkType.SENTENCE,  content = sentence.toString()))
                }
                yield(Chunk(type=ChunkType.SENTENCE, command = ChunkCommand.LAST))
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

    override fun metadataFor(record: DataRecord): Metadata {
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
            return Metadata(values=metadata,createdBy = name)

        }
        // create an empty Annotation just with the given text

        return Metadata()
    }

}



fun documentRepresenation(record: DataRecord, textOutCapabilityName:String, textOutCapabilityPathName:String,
                          lookup:CapabilityLookupStrategy, name:String,
                          contHandlerCreator: (outputStream: OutputStream) -> ContentHandler):DocumentRepresentation {
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
        return DocumentRepresentation(outPath,name)
    }
    return DocumentRepresentation()
}

@RequiresCapabilities(originalContentIn,htmlTextOut, htmlTextOutPath)
class TikaHtmlDocumentRepresentationProducer(val lookup:CapabilityLookupStrategy, override val name:String = "tika-html"):DocumentRepresentationProducer {
    override fun documentRepresentationFor(record: DataRecord): DocumentRepresentation {
        return documentRepresenation(record, htmlTextOut, htmlTextOutPath,lookup,"tika-html",
                {out ->
                    ToHTMLContentHandler(out,"utf-8")})
    }
}

@RequiresCapabilities(originalContentIn, simpleTextOut, simpleTextOutPath)
class TikaTxtDocumentRepresentationProducer(val lookup:CapabilityLookupStrategy, override val name:String = "tika-txt"):DocumentRepresentationProducer {
    override fun documentRepresentationFor(record: DataRecord): DocumentRepresentation {
        return documentRepresenation(record, simpleTextOut, simpleTextOutPath,lookup,"tika-txt",
                {out ->  ToTextContentHandler(out,"utf-8")})
    }
}

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
        Capability<String?> {
    override val name = "tika-metadata"


    override fun  execute(name:String, dataRecord: DataRecord): String? {
        return dataRecord.meta.filter { it.createdBy == name }.firstOrNull()?.values?.get("lang")?:extractLang(dataRecord)
    }

    fun extractLang(record: DataRecord):String? {
        val text =  lookup.lookup(simpleTextIn, record, String::class.java)
        if(StringUtils.isNotEmpty(text)) {
            val language = LanguageIdentifier(text)?.language
            return language
        }
        return null
    }


    override fun metadataFor(record: DataRecord): Metadata {
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
                metadata.set("lang", extractLang(record))
            }
        }
        metadata.names().forEach { name ->
            metadataPipeline.put(name,metadata.get(name))
        }
        if(metadataPipeline.size>0)  return Metadata(metadataPipeline, name)
        else return Metadata()
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


    override fun metadataFor(record: DataRecord): Metadata {
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
