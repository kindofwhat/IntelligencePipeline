package pipeline.capabilities

import datatypes.DataRecord
import kotlinx.io.InputStream

const val originalFileContent = "originalFileContent"
const val metadata = "metadata"
const val simpleText = "simpleText"
const val htmlText = "htmlText"
const val languageDetection = "languageDetection"

@Target(AnnotationTarget.CLASS, AnnotationTarget.VALUE_PARAMETER)
@MustBeDocumented
annotation class RequiresCapabilities(vararg val name:String)

@Target(AnnotationTarget.CLASS, AnnotationTarget.VALUE_PARAMETER)
@MustBeDocumented
annotation class HasCapabilities(vararg val name:String)

interface Capability <T>{
    fun retrieve(name:String, dataRecord: DataRecord):T
}

@HasCapabilities(originalFileContent)
interface BinaryCapability: Capability<InputStream?>  {
    override fun retrieve(name:String, dataRecord: DataRecord): InputStream?
}


interface FullTextCapability: Capability<String>  {
    override fun retrieve(name:String, dataRecord: DataRecord): String
}
@HasCapabilities(simpleText)
interface SimpleTextCapability: FullTextCapability
@HasCapabilities(htmlText)
interface HtmlTextCapability: FullTextCapability
@HasCapabilities(languageDetection)
interface LanguageDetectionCapability:  Capability<String>



interface CapabilityRegistry {
    fun register(capability: Capability<*>)
}

interface CapabilityLookup {
    fun requestCapability(name: String): Set<Capability<*>>
}

interface CapabilityLookupStrategy {
    fun <T> lookup(capability: String, dataRecord: DataRecord, clazz:Class<T>): T?
}