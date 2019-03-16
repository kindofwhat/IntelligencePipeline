package pipeline.capabilities

import kotlin.reflect.KClass
import kotlin.reflect.full.allSuperclasses

class DefaultCapabilityRegistry: CapabilityRegistry, CapabilityLookup, CapabilityLookupStrategy {
    private var registry = mutableMapOf<String, Set<Capability<*>>>()
    /**
     * returns the first capability if found
     */
    override fun <T> lookup(capability: String, dataRecord: datatypes.DataRecord, clazz: Class<T>): T? {
        return requestCapability(capability).
                map { cap ->  cap.execute(capability, dataRecord)}
                .filter { result -> result != null }
                .filter { result -> clazz.isAssignableFrom(result!!::class.java)}
                .map { result -> result as T? }
                .firstOrNull()
    }

    override fun requestCapability(name: String): Set<Capability<*>> {
        return registry.get(name).orEmpty()
    }

    override fun register(capability: Capability<*>) {
        val clazzes = mutableListOf<KClass<*>>(capability::class)
        clazzes.addAll(capability::class.allSuperclasses)
        clazzes.forEach { clazz ->
            clazz.annotations.filter { annotation -> annotation.annotationClass.qualifiedName == HasCapabilities::class.qualifiedName }
            .forEach { annotation ->
                val names = (annotation as HasCapabilities).name
                names.forEach {name ->
                    registry.set(name,registry.get(name).orEmpty() + capability) }

            }
        }
    }
}