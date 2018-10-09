package pipeline.serialize

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JSON
import kotlinx.serialization.serializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.Charset

inline fun <reified T : Any> deserialize(value: ByteArray): T {
    return JSON.parse<T>(String(value))
}


inline fun <reified T : Any> serialize(value: T):ByteArray {
    return JSON.stringify(value).toByteArray(Charset.forName("utf-8"))
}


class KotlinSerializer<T:Any> :Serializer<T?> {
    override fun serialize(topic: String?, data: T?): ByteArray? {
        if(data == null) return null
        return JSON.stringify(data::class.serializer() as KSerializer<T>,data)
                .toByteArray(Charset.forName("utf-8"))
    }
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun close() {}
}

class KotlinDeserializer<T:Any>(obj: Class<T>) :Deserializer<T> {
    private var instance:KSerializer<out T> = obj.newInstance()::class.serializer()
    companion object {
        val CLASSNAME= "CLASSNAME"
    }
    override fun deserialize(topic: String?, data: ByteArray?): T? {
        if(data == null) return null
        return JSON.parse( instance, String(data))
    }
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {

    }
    override fun close() {}
}

open class KotlinSerde<T:Any>(obj: Class<T>): Serde<T> {
    val serializer = KotlinSerializer<T>()
    val deserializer = KotlinDeserializer(obj)

    override fun close() {
        serializer.close()
        deserializer.close()
    }
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
       serializer.configure(configs,isKey)
        deserializer.configure(configs,isKey)
    }

    override fun serializer(): Serializer<T?> {
        return serializer;
    }

    override fun deserializer(): Deserializer<T> {
        return deserializer
    }
}