package unittests

import datatypes.DataRecord
import org.junit.Test
import pipeline.capabilities.*

@HasCapabilities("test1")
class TestCapabilty1:Capability<String> {
    override fun retrieve(name:String, dataRecord: DataRecord): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

@HasCapabilities("test2")
class TestCapabilty2:Capability<String> {
    override fun retrieve(name:String, dataRecord: DataRecord): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

@HasCapabilities("test1")
class TestCapabilty1Int:Capability<Int> {
    override fun retrieve(name:String, dataRecord: DataRecord): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class TestLanguageDetectionCapability:LanguageDetectionCapability,SimpleTextCapability {
    override fun retrieve(name: String, dataRecord: DataRecord): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
class RegistryTests {
    @Test
    fun testOne() {
        val registry=DefaultCapabilityRegistry()
        registry.register(TestCapabilty1())
        assert(registry.requestCapability("test1").size == 1)
        assert(registry.requestCapability("test2").size == 0)
    }

    @Test
    fun test2() {
        val registry=DefaultCapabilityRegistry()
        registry.register(TestCapabilty1())
        registry.register(TestCapabilty2())
        assert(registry.requestCapability("test1").size == 1)
        assert(registry.requestCapability("test2").size == 1)
    }

    @Test
    fun testWithDifferentReturnValues() {
        val registry=DefaultCapabilityRegistry()
        registry.register(TestCapabilty1())
        registry.register(TestCapabilty1Int())
        assert(registry.requestCapability("test1").size == 2)
    }

    @Test
    fun testDoNOTpreventDoubleRegistration() {
        val registry=DefaultCapabilityRegistry()
        registry.register(TestCapabilty1())
        registry.register(TestCapabilty1())
        assert(registry.requestCapability("test1").size == 2)
    }

    @Test
    fun testIndirectAnnotations() {
        val registry=DefaultCapabilityRegistry()
        registry.register(TestLanguageDetectionCapability())
        assert(registry.requestCapability(languageDetection).size == 1)
        assert(registry.requestCapability(simpleText).size == 1)
        assert(registry.requestCapability(htmlText).size == 1)
    }

}