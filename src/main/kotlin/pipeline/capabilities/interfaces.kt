package pipeline.capabilities

import datatypes.DataRecord
import kotlinx.io.InputStream
import participants.PipelineSideEffect
import java.io.OutputStream
import java.io.Writer


const val languageDetection = "languageDetection"

const val originalContentIn = "originalContentIn"
const val simpleTextIn = "simpleTextIn"
const val htmlTextIn = "htmlTextIn"

const val textOut = "textOut"
const val simpleTextOut = "simpleTextOut"
const val htmlTextOut = "htmlTextOut"

@Target(AnnotationTarget.CLASS, AnnotationTarget.VALUE_PARAMETER)
@MustBeDocumented
annotation class RequiresCapabilities(vararg val name:String)

@Target(AnnotationTarget.CLASS, AnnotationTarget.VALUE_PARAMETER)
@MustBeDocumented
annotation class HasCapabilities(vararg val name:String)

interface Capability <T>{
    fun execute(name:String, dataRecord: DataRecord): T
}

@HasCapabilities(originalContentIn) interface OriginalContentCapability: Capability<InputStream?>

interface FullTextCapabilityIn: Capability<String?>
@HasCapabilities(simpleTextIn) interface TxtTextCapabilityIn: FullTextCapabilityIn
@HasCapabilities(htmlTextIn) interface HtmlTextCapabilityIn: FullTextCapabilityIn

interface TextCapabilityOut: Capability<OutputStream?>
@HasCapabilities(simpleTextOut) interface TxtTextCapabilityOut: TextCapabilityOut
@HasCapabilities(htmlTextOut) interface HtmlCapabilityOut: TextCapabilityOut


@HasCapabilities(languageDetection) interface LanguageDetectionCapability:  Capability<String?>


interface CapabilityRegistry {
    fun register(capability: Capability<*>)
}

interface CapabilityLookup {
    fun requestCapability(name: String): Set<Capability<*>>
}

interface CapabilityLookupStrategy {
    fun <T> lookup(capability: String, dataRecord: DataRecord, clazz:Class<T>): T?
}