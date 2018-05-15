package pipeline.capabilities


const val languageDetection = "languageDetection"

const val originalContentIn = "originalContentIn"
const val simpleTextIn = "simpleTextIn"
const val htmlTextIn = "htmlTextIn"

const val simpleTextOut = "simpleTextOut"
const val simpleTextOutPath = "simpleTextOutPath"
const val htmlTextOut = "htmlTextOut"
const val htmlTextOutPath = "htmlTextOutPath"

@Target(AnnotationTarget.CLASS, AnnotationTarget.VALUE_PARAMETER)
@MustBeDocumented
annotation class RequiresCapabilities(vararg val name:String)

@Target(AnnotationTarget.CLASS, AnnotationTarget.VALUE_PARAMETER)
@MustBeDocumented
annotation class HasCapabilities(vararg val name:String)

interface Capability <T>{
    fun execute(name:String, dataRecord: datatypes.DataRecord): T
}

interface CapabilityRegistry {
    fun register(capability: Capability<*>)
}

interface CapabilityLookup {
    fun requestCapability(name: String): Set<Capability<*>>
}

interface CapabilityLookupStrategy {
    fun <T> lookup(capability: String, dataRecord: datatypes.DataRecord, clazz:Class<T>): T?
}