package orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.executor.OResult
import kotlinx.serialization.*
import kotlinx.serialization.json.Json
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure


fun schemaPersist(session: ODatabaseSession, obj: Any): ODocument {
    val record = session.newInstance<ODocument>(obj::class.simpleName)
    obj::class.declaredMemberProperties.forEach { property ->
        if (isSimpleProperty(property)) {
            record.setProperty(property.name, readProperty(obj, property.name))
        } else if (isObjectProperty(property)) {
            val objRecord = schemaPersist(session, readProperty(obj, property.name))
            record.setProperty(property.name,objRecord.identity)
        } else if(isCollectionType(property)) {
            if (property.returnType.isSubtypeOf(List::class.starProjectedType)) {
                val listClass = property.returnType.arguments.first().type?.jvmErasure

                if (listClass?.qualifiedName?.startsWith("kotlin") ?: true) {
                    //primitive type
                    record.setProperty(property.name, readProperty(obj,property.name))
                } else {
                    val values = readProperty<List<*>>(obj,property.name)
                            .filter { it !=null}
                            .map { schemaPersist(session,it!!)}
                            .map { it.identity }
                    record.setProperty(property.name,values)
                    //custom type
                    /*
                    val values = record.getProperty<List<*>>(property.name)
                            .filter { it!=null }
                            .map { value -> schemaPersist(session,value!!) }
                            .map { it.identity}

                    record.setProperty(property.name,values)
                    */
                }
            }
        }
    }
    session.save<ODocument>(record)
    return record
}

fun <R : Any?> readProperty(instance: Any, propertyName: String): R {
    val clazz = instance.javaClass.kotlin
    @Suppress("UNCHECKED_CAST")
    return clazz.declaredMemberProperties.first { it.name == propertyName }.get(instance) as R
}

fun createReflectedSchema(session: ODatabaseSession, clazz: KClass<*>?, stack: MutableMap<String, OClass> = mutableMapOf()) {
    if (!stack.containsKey(clazz?.simpleName ?: "")) {
        val myClass = session.createClass(clazz?.simpleName ?: "")
        stack.set(clazz?.simpleName ?: "", myClass)

        clazz?.memberProperties?.forEach { property ->
            if (isSimpleProperty(property)) {
                createReflectedProperty(property, myClass)
            } else if (isObjectProperty(property)) {
                val objectClass = property.returnType.jvmErasure
                createReflectedSchema(session, objectClass, stack)
                myClass.createProperty(property.name, OType.LINK, stack.get(objectClass.simpleName))


            } else if (isCollectionType(property)) {
                if (property.returnType.isSubtypeOf(List::class.starProjectedType)) {
                    val listClass = property.returnType.arguments.first().type?.jvmErasure

                    if (listClass?.qualifiedName?.startsWith("kotlin") ?: true) {
                        //primitive type
                        myClass.createProperty(property.name, OType.EMBEDDEDLIST)
                    } else {
                        //custom type
                        createReflectedSchema(session, listClass, stack)
                        myClass.createProperty(property.name, OType.LINKLIST, stack.get(listClass?.simpleName))

                    }
                }
            }
        }

    }
}

fun isObjectProperty(property: KProperty1<out Any, Any?>): Boolean {
    //TODO: have here whitelisted packages
    return !isCollectionType(property) && !isSimpleProperty(property)
}

private fun isCollectionType(property: KProperty1<out Any, Any?>): Boolean {
    //TODO: have here whitelisted packages
    return property.returnType.isSubtypeOf(Collection::class.starProjectedType)
}

private fun isSimpleProperty(property: KProperty1<out Any, Any?>): Boolean {
    return property.returnType.toString().startsWith("kotlin") && !isCollectionType(property)
}

private fun createReflectedProperty(property: KProperty1<out Any, Any?>, myClass: OClass?) {
    when (property.returnType.toString()) {
        "kotlin.Boolean" -> myClass?.createProperty(property.name, OType.BOOLEAN)
        "kotlin.String" -> myClass?.createProperty(property.name, OType.STRING)
        "kotlin.ByteArray" -> myClass?.createProperty(property.name, OType.BYTE)
        "kotlin.Double" -> myClass?.createProperty(property.name, OType.DOUBLE)
        "kotlin.Float" -> myClass?.createProperty(property.name, OType.FLOAT)
        "kotlin.Integer" -> myClass?.createProperty(property.name, OType.INTEGER)
        "kotlin.Long" -> myClass?.createProperty(property.name, OType.LONG)
    }
}


fun createSchema(session: ODatabaseSession, descriptor: SerialDescriptor, dbClass: OClass? = null, stack: MutableMap<String, OClass> = mutableMapOf()) {
    when (descriptor.kind) {
        is StructureKind.CLASS -> {
            if (!stack.containsKey(descriptor.name)) {
                val myClass = session.createClass(descriptor.name)
                stack.set(descriptor.name, myClass)
                descriptor.elementDescriptors().forEach { createSchema(session, it, myClass, stack) }
            }

        }
        is StructureKind.LIST -> {
            //
            val firstDescriptor = descriptor.elementDescriptors().getOrNull(0)
            when (firstDescriptor?.kind) {
                is StructureKind.CLASS -> {
                    createSchema(session, firstDescriptor, null, stack)
                    dbClass?.createProperty(firstDescriptor.name, OType.LINK, stack.get(firstDescriptor.name))
                }
                else -> {
                    dbClass?.createProperty(descriptor.name, OType.LINKLIST)
                }
            }
        }
        is PrimitiveKind.BOOLEAN -> dbClass?.createProperty(descriptor.name, OType.BOOLEAN)
        is PrimitiveKind.STRING -> dbClass?.createProperty(descriptor.name, OType.STRING)
        is PrimitiveKind.BYTE -> dbClass?.createProperty(descriptor.name, OType.BYTE)
        is PrimitiveKind.DOUBLE -> dbClass?.createProperty(descriptor.name, OType.DOUBLE)
        is PrimitiveKind.FLOAT -> dbClass?.createProperty(descriptor.name, OType.FLOAT)
        is PrimitiveKind.LONG -> dbClass?.createProperty(descriptor.name, OType.LONG)
        is PrimitiveKind.INT -> dbClass?.createProperty(descriptor.name, OType.INTEGER)
    }
}


fun createOrientDocumentClassWithIndex(session: ODatabaseSession, className: String, indexName: String) {
    if (session.getClass(className) == null) {
        session.activateOnCurrentThread()
        val myClass = session.createClass(className)
        myClass.createProperty(indexName, OType.STRING)
        myClass.createIndex("${className}_idx_$indexName", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, indexName)
        session.commit()
    }

}


@ImplicitReflectionSerializer
private fun <T : Any> loadOrCreate(session: ODatabaseSession, data: T?, indexFieldName: String): ODocument? {
    if (data == null) return null
    val document = toDocument(data)
    if (document != null) {
        val result = session.query("SELECT FROM ${document.className} WHERE ${indexFieldName} = '${document.field<String>(indexFieldName)}'")
        if (result.hasNext()) {
            val res = result.next()
            val existingDocument = res.toElement() as ODocument

            document.fieldNames().forEach { field ->
                existingDocument.field(field, document.field<Any>(field))
            }
            result.close()
            return existingDocument
        }
    }
    return document
}

@ImplicitReflectionSerializer
fun <T : Any> toDocument(data: T?): ODocument? {
    if (data == null) return null
    val document = ODocument(data::class.simpleName)
    return document.fromJSON(Json.stringify(data::class.serializer() as KSerializer<T>, data))
}

@ImplicitReflectionSerializer
fun <T : Any> persist(session: ODatabaseSession, data: T?, indexFieldName: String) {
    if (data == null) return
    session.activateOnCurrentThread()

    session.executeWithRetry(10) { session ->
        val document = loadOrCreate(session, data, indexFieldName)
        session.save<ODocument>(document)

    }
}


@ImplicitReflectionSerializer
fun <T : Any> toObject(data: OResult?, obj: Class<T>): T? {
    val serializer: KSerializer<out T> = obj.newInstance()::class.serializer()
    val dataRecord = Json.nonstrict.parse(serializer, data?.toElement()?.toJSON() ?: "")
    return dataRecord
}