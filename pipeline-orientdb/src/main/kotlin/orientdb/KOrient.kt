package orientdb

import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.exception.OSchemaException
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.executor.OResult
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.launch
import kotlinx.serialization.*
import kotlinx.serialization.json.Json
import org.bouncycastle.cms.RecipientId.password
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure

typealias  Loader<T> = (session: ODatabaseSession, T) -> ODocument?


class FieldLoader(val fieldName: String) : Loader<Any> {
    override fun invoke(session: ODatabaseSession, p2: Any): ODocument? {
        val className = p2::class.simpleName ?: ""
        val fieldValue = readProperty<Any>(p2, fieldName)
        val result = session.query("SELECT FROM ${className} WHERE ${fieldName} = '${fieldValue}'")
        if (result.hasNext()) {
            val res = result.next()
            val existingDocument = res.toElement() as ODocument
            result.close()
            return existingDocument
        }
        return null
    }
}

@ImplicitReflectionSerializer
class KOrient(connection:String, dbName:String, user:String, password:String, var loaders: MutableMap<String, Loader<Any>> = mutableMapOf()) {

    private val stack: MutableMap<String, OClass> = mutableMapOf()


    private val orient: OrientDB
    private val pool:ODatabasePool

    init {

        orient = OrientDB(connection, user, password, OrientDBConfig.defaultConfig())
        pool = ODatabasePool(orient, dbName, user, password)
    }

    fun <T : Any> save(obj: T): ODocument? {
        val session = pool.acquire()
        val record = createDocument(session, obj)
        session.activateOnCurrentThread()
        session.executeWithRetry(10) { mySession ->
            mySession.activateOnCurrentThread()
            mySession.save<ODocument>(record)
        }
        session.close()
        return record
    }

    fun <T : Any> queryAll(clazz: Class<T>): ReceiveChannel<T> {
        val channel = Channel<T>()

        //todo: custom scope
        GlobalScope.launch {
            val session = pool.acquire()
            session.activateOnCurrentThread()
            val res = session.query("SELECT FROM ${clazz.simpleName}")
            while (res.hasNext()) {
                val maybeDataRecord = toObject<T>(res.next(), clazz)
                if (maybeDataRecord != null) {
                    channel.send(maybeDataRecord)
                }
            }
            channel.close()
            res.close()
            session.close()
        }
        return channel
    }

    fun createSchema( clazz: KClass<*>?) {
        val session = pool.acquire()
        session.activateOnCurrentThread()
        internalCreateSchema(session,clazz)
        session.close()
    }
    private fun internalCreateSchema(session: ODatabaseSession, clazz: KClass<*>?) {
        val name = clazz?.simpleName
        if (!stack.containsKey(name ?: "") && loaders.containsKey(name)) {

            try {
                session.getMetadata().getSchema().dropClass(name ?:"");
            } catch (e:OSchemaException){}
            val myClass = session.createClass(name ?: "")
            stack.set(name ?: "", myClass)

            clazz?.memberProperties?.forEach { property ->
                if (isSimpleProperty(property)) {
                    createReflectedProperty(property, myClass)
                } else if (isObjectProperty(property)) {
                    val objectClass = property.returnType.jvmErasure
                    if (!loaders.containsKey(objectClass.simpleName)) {
                        myClass.createProperty(property.name, OType.EMBEDDED)

                    } else {
                        internalCreateSchema(session,objectClass)
                        myClass.createProperty(property.name, OType.LINK, stack.get(objectClass.simpleName))

                    }
                } else if (isCollectionType(property)) {
                    if (property.returnType.isSubtypeOf(List::class.starProjectedType)) {
                        createCollectionProperty(session, stack, OType.EMBEDDEDLIST, OType.LINKLIST, myClass, property)
                    } else if (property.returnType.isSubtypeOf(Map::class.starProjectedType)) {
                        createCollectionProperty(session, stack, OType.EMBEDDEDMAP, OType.LINKMAP, myClass, property)
                    } else if (property.returnType.isSubtypeOf(Set::class.starProjectedType)) {
                        createCollectionProperty(session, stack, OType.EMBEDDEDSET, OType.LINKSET, myClass, property)
                    }

                }
            }

        }
    }

    @ImplicitReflectionSerializer
    fun <T : Any> toObject(data: OResult?, obj: Class<T>): T? {
        val serializer: KSerializer<out T> = obj.newInstance()::class.serializer()
        val dataRecord = Json.nonstrict.parse(serializer, data?.getProperty(JSON_PROPERTY_NAME) ?: "")
        return dataRecord
    }

    //////////////////private/////////////////////////77

    private fun <T : Any> createDocument(session: ODatabaseSession, obj: T): ODocument? {
        val loader = loaders.get(obj::class.simpleName)
        val record: ODocument
        if (loader == null) {
            record = session.newInstance<ODocument>(obj::class.simpleName)
        } else {
            record = loader(session, obj) ?: session.newInstance<ODocument>(obj::class.simpleName)
        }
        obj::class.declaredMemberProperties.forEach { property ->
            handleProperty(session, record, property, obj)
        }
        record.setProperty(JSON_PROPERTY_NAME, Json.stringify(obj::class.serializer() as KSerializer<T>, obj))
        return record
    }

    private fun <T : Any> handleProperty(session: ODatabaseSession, record: ODocument, property: KProperty1<out Any, Any?>, obj: T) {
        val readProperty = readProperty<Any>(obj, property.name)
        val readPropertyClass = readProperty::class
        if (isSimpleProperty(property)) {
            record.setProperty(property.name, readProperty)
        } else if (isObjectProperty(property)) {
            if (!loaders.containsKey(readPropertyClass.simpleName)) {
                val objRecord = createDocument(session, readProperty)
                record.setProperty(property.name, objRecord)
            } else {
                val objRecord = save( readProperty)
                record.setProperty(property.name, objRecord?.identity)

            }
        } else if (isCollectionType(property)) {

            if (property.returnType.isSubtypeOf(List::class.starProjectedType) || property.returnType.isSubtypeOf(Set::class.starProjectedType)) {
                val collectionClass = property.returnType.arguments.first().type?.jvmErasure
                if (collectionClass?.qualifiedName?.startsWith("kotlin") ?: true) {
                    record.setProperty(property.name, readProperty)
                } else if (!loaders.containsKey(collectionClass?.simpleName)) {
                    if (property.returnType.isSubtypeOf(List::class.starProjectedType)) {
                        val values = readProperty<List<*>>(obj, property.name)
                                .filter { it != null }
                                .map { createDocument(session, it!!) }
                        record.setProperty(property.name, values)

                    } else {
                        val values = readProperty<Set<*>>(obj, property.name)
                                .filter { it != null }
                                .map { createDocument(session, it!!) }.toSet()
                        record.setProperty(property.name, values)
                    }
                } else {
                    if (property.returnType.isSubtypeOf(List::class.starProjectedType)) {
                        val values = readProperty<List<*>>(obj, property.name)
                                .filter { it != null }
                                .map { save( it!!) }
                                .map { it?.identity }
                        record.setProperty(property.name, values)

                    } else {
                        val values = readProperty<Set<*>>(obj, property.name)
                                .filter { it != null }
                                .map { save( it!!) }
                                .map { it?.identity }.toSet()
                        record.setProperty(property.name, values)
                    }
                }
            } else if (property.returnType.isSubtypeOf(Map::class.starProjectedType)) {
                val collectionClass = property.returnType.arguments.get(1).type?.jvmErasure
                if (collectionClass?.qualifiedName?.startsWith("kotlin") ?: true) {
                    record.setProperty(property.name, readProperty)
                } else if (!loaders.containsKey(collectionClass?.simpleName)) {
                    val values = readProperty<Map<String, *>>(obj, property.name)
                            .map { Pair("${it.key}", createDocument(session, it.value!!)) }
                            .toMap()
                    record.setProperty(property.name, values)
                } else {
                    val values = readProperty<Map<*, *>>(obj, property.name)
                            .filter { it.value != null }
                            .map { Pair("${it.key}", save( it.value!!)?.identity) }
                            .toMap()
                    record.setProperty(property.name, values)
                }
            }
        } else if (!loaders.containsKey(readPropertyClass.simpleName)) {
            //don't know how to retrieve object => save it serialized

            record.setProperty(property.name, readProperty)
        }
    }

    private fun createCollectionProperty(session: ODatabaseSession, stack: MutableMap<String, OClass>,
                                         simpleType: OType, collectionType: OType, myClass: OClass?, property: KProperty1<out Any, Any?>) {

        val listClass: KClass<*>?



        if (property.returnType.isSubtypeOf(Map::class.starProjectedType)) {
            listClass = property.returnType.arguments.get(1).type?.jvmErasure
        } else {
            listClass = property.returnType.arguments.first().type?.jvmErasure
        }

        if (!loaders.containsKey(listClass?.simpleName)) {
            myClass?.createProperty(property.name, simpleType, stack.get(listClass?.simpleName))
        } else {
            if (listClass?.qualifiedName?.startsWith("kotlin") ?: true) {
                //primitive type
                myClass?.createProperty(property.name, simpleType)
            } else {
                //custom type
                internalCreateSchema(session, listClass)
                myClass?.createProperty(property.name, collectionType, stack.get(listClass?.simpleName))

            }
        }

    }


    private fun isObjectProperty(property: KProperty1<out Any, Any?>): Boolean {
        //TODO: have here whitelisted packages
        return !isCollectionType(property) && !isSimpleProperty(property)
    }

    private fun isCollectionType(property: KProperty1<out Any, Any?>): Boolean {
        //TODO: have here whitelisted packages
        return property.returnType.isSubtypeOf(Collection::class.starProjectedType) || property.returnType.isSubtypeOf(Map::class.starProjectedType)
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

    companion object {
        public const val JSON_PROPERTY_NAME = "__json__"
    }
}

fun <R : Any?> readProperty(instance: Any, propertyName: String): R {
    val clazz = instance.javaClass.kotlin
    @Suppress("UNCHECKED_CAST")
    return clazz.declaredMemberProperties.first { it.name == propertyName }.get(instance) as R
}


