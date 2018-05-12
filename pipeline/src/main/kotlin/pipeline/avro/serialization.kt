package pipeline.avro

import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectDatumReader
import org.apache.avro.reflect.ReflectDatumWriter
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

inline fun <reified T> deserialize(value: ByteArray): T {
    val reader = ReflectDatumReader<T>(T::class.java)
    val result =T::class.java.newInstance()
    reader.read(result, DecoderFactory.get().directBinaryDecoder(ByteArrayInputStream(value), null))
    return result
}


inline fun <reified T> serialize(value: T):ByteArray {
    val out = ByteArrayOutputStream()
    val writer = ReflectDatumWriter<T>(T::class.java)
    writer.write(value, EncoderFactory.get().directBinaryEncoder(out, null))
    return out.toByteArray()
}

