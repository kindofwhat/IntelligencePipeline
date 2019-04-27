package orientdb

import com.orientechnologies.common.concur.ONeedRetryException
import com.orientechnologies.orient.core.config.OGlobalConfiguration
import com.orientechnologies.orient.core.db.ODatabasePool
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.exception.OConcurrentModificationException
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.executor.OResult
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.jvm.jvmName

typealias  Loader<T> = (session: ODatabaseSession, T) -> ODocument?


class FieldLoader(val fieldName: String) : Loader<Any> {
    override fun invoke(session: ODatabaseSession, p2: Any): ODocument? {
        val className = p2::class.simpleName ?: ""
        val fieldValue = readProperty<Any>(p2, fieldName)
        session.activateOnCurrentThread()
        session.reload()
        val result = session.query("SELECT FROM ${className} WHERE ${fieldName} = ? NOCACHE", fieldValue)
        if (result.hasNext()) {
            val res = result.next()
            val existingDocument = res.toElement() as ODocument
            result.close()
            return existingDocument
        }
        return null
    }
}

class KOrient(connection: String, val dbName: String, val user: String, val password: String, var loaders: MutableMap<String, Loader<Any>> = mutableMapOf()) {

    private val CLASS_HINT =  "__class_hint__"
    private val nameToDBClass: MutableMap<String, OClass> = mutableMapOf()
    private val nameToKotlinClass: MutableMap<String, KClass<Any>> = mutableMapOf()


    private val orient: OrientDB
    private val pool: ODatabasePool

    init {

        OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINER")
        OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.setValue(3000)
        val config = OrientDBConfig.defaultConfig()
        orient = OrientDB(connection, user, password, config)

        pool = ODatabasePool(orient, dbName, user, password)
    }


    fun <T : Any> save(obj: T?): T? {
        if (obj == null) return null
        val session = pool.acquire()
        try {
            return toObject(saveDocument(obj, rootIds = mutableSetOf(), session = session ,save=true), obj::class as KClass<T>)

        } finally {
            session.activateOnCurrentThread()
            session.close()
        }

    }

    fun <T : Any> load(obj: T): T? {
        val session = pool.acquire()
        try {
            return toObject(this.loaders.get(obj::class.simpleName)?.invoke(session, obj), obj::class as KClass<T>)

        } finally {
            session.activateOnCurrentThread()
            session.close()
        }
    }

    fun <T : Any> queryAll(clazz: KClass<T>): ReceiveChannel<T> {
        val channel = Channel<T>()

        //todo: custom scope
        GlobalScope.launch {
            val session =  pool.acquire()
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
            session.activateOnCurrentThread()
            session.close()
        }
        return channel
    }

    fun createSchema(clazz: KClass<*>?) {
        val session = pool.acquire()
        session.activateOnCurrentThread()
        internalCreateSchema(session, clazz)
        session.commit()
        session.close()
    }

    private fun internalCreateSchema(session: ODatabaseSession, clazz: KClass<*>?): OClass? {
        val name = clazz?.simpleName
        if (!nameToDBClass.containsKey(name ?: "") && loaders.containsKey(name)) {
            val schema = session.getMetadata().getSchema()
            var myClass: OClass? = schema.getClass(name)

            if(myClass == null) {
                val superClasses = clazz?.superclasses?.map { superClazz ->
                    internalCreateSchema(session, superClazz)
                }?.filterNotNull()?.map { it?.name }?.toTypedArray()

                if (superClasses != null && superClasses.size > 0) {
                    myClass = session.createClass(name ?: "", *superClasses)
                } else {
                    myClass = session.createClass(name ?: "")
                }
            }
            if(myClass!=null)  {
                nameToDBClass.set(name ?: "", myClass)
                if( myClass.properties().find { property -> property.name == CLASS_HINT } == null)
                  myClass.createProperty(CLASS_HINT,OType.STRING)
            }
            nameToKotlinClass.set(name ?: "", clazz as KClass<Any>)


            clazz.memberProperties
                    .filter { property -> myClass?.properties()?.find { it.name == property.name } == null }
                    .forEach { property ->
                        if (isSimpleProperty(property)) {
                            createReflectedProperty(property, myClass)
                        } else if (isObjectProperty(property)) {
                            val objectClass = property.returnType.jvmErasure
                            if (!loaders.containsKey(objectClass.simpleName)) {
                                myClass?.createProperty(property.name, OType.EMBEDDED)
                                nameToKotlinClass.set(objectClass.simpleName ?: "", objectClass as KClass<Any>)
                            } else {
                                internalCreateSchema(session, objectClass)
                                myClass?.createProperty(property.name, OType.LINK, nameToDBClass.get(objectClass.simpleName))

                            }
                        } else if (isCollectionType(property)) {
                            if (property.returnType.isSubtypeOf(List::class.starProjectedType)) {
                                createCollectionProperty(session, nameToDBClass, OType.EMBEDDEDLIST, OType.LINKLIST, myClass, property)
                            } else if (property.returnType.isSubtypeOf(Map::class.starProjectedType)) {
                                createCollectionProperty(session, nameToDBClass, OType.EMBEDDEDMAP, OType.LINKMAP, myClass, property)
                            } else if (property.returnType.isSubtypeOf(Set::class.starProjectedType)) {
                                createCollectionProperty(session, nameToDBClass, OType.EMBEDDEDSET, OType.LINKSET, myClass, property)
                            }

                        }
                    }

        }
        return nameToDBClass.get(name)
    }


    private fun <T : Any> internalToObject(data: OElement?, clazz: KClass<T>, session: ODatabaseSession, loadedStack: MutableMap<OIdentifiable, Any?>): T? {
        if (data == null) return null
        val values = mutableMapOf<String, Any?>()

        //loop prevention
        var instance = clazz.createInstance()
        loadedStack.put(data.identity,instance)
        data.propertyNames.map { propertyName ->
            Pair(propertyName, data.getProperty<Any>(propertyName))
        }.forEach { (name, property) ->
            values.put(name, internalLoadProperty(session, property, loadedStack))
        }

        /*
        //TODO: more sophisticated solution here? i.e. check for "best machting" constructor, or try to set fields if writable
        val constructor = clazz.constructors.first()
        val params = constructor.parameters.associateBy({ it }, { values.get(it.name) })
        val myInstance = constructor.callBy(params)

         */
        val copy = instance::class.memberFunctions.first { it.name == "copy" }
        val params = copy.parameters.associateBy({ it }, {
            if(copy.instanceParameter ==it) {
               instance
            }  else {
                values.get(it.name)
            }
        })
        instance = copy.callBy(params) as T
        loadedStack.put(data.identity, instance)

        return instance


    }

    private fun internalLoadProperty(session: ODatabaseSession, dbProperty: Any?, loadedStack: MutableMap<OIdentifiable, Any?>): Any? {
        var result: Any? = null

        when (dbProperty) {
            is Set<*> -> {
                result = dbProperty.map { oneProperty -> internalLoadProperty(session, oneProperty, loadedStack) }.toSet()
            }
            is List<*> -> {
                result = dbProperty.map { oneProperty -> internalLoadProperty(session, oneProperty, loadedStack) }.toList()
            }
            is Map<*, *> -> {
                result = dbProperty.entries.associate { property ->
                    property.key to internalLoadProperty(session, property.value, loadedStack)
                }
            }
            is OIdentifiable -> {

                if (loadedStack.containsKey(dbProperty)) {
                    result = loadedStack.get(dbProperty)
                } else {
                    if (dbProperty is ODocument && (dbProperty.isEmbedded || dbProperty.identity.isNew)) {
                        result = internalToObject(dbProperty, Class.forName(dbProperty.getProperty(CLASS_HINT)).kotlin, session, loadedStack)

                    } else {
                        session.activateOnCurrentThread()
                        val res = session.query("SELECT FROM ${dbProperty.identity}").next().toElement() as ODocument
                        result = internalToObject(res,
                                nameToKotlinClass.get(res.className)!!, session, loadedStack)

                    }
                }
                //if(loaders.containsKey(memberProperty.returnType.classifier.))
            }
            else -> {
                result = dbProperty

            }
        }
        return result
    }

    fun <T : Any> toObject(data: OResult?, obj: KClass<T>): T? {
        val session =  pool.acquire()
        try {
            return internalToObject(data?.toElement(), obj, session, mutableMapOf())
        } finally {
            session.close()
        }

    }

    fun <T : Any> toObject(data: ODocument?, obj: KClass<T>): T? {
        val session =  pool.acquire()
        try {
            return internalToObject(data, obj, session, mutableMapOf())
        } finally {
            session.close()
        }
    }

    //////////////////private/////////////////////////77

    private fun <T : Any> createDocument(session: ODatabaseSession, obj: T, save:Boolean = false, rootIds: MutableSet<ORID>): ODocument? {
        val loader = loaders.get(obj::class.simpleName)
        var mySave = save
        var record: ODocument
        if (loader == null) {
            record = session.newElement().getRecord()
        } else {
            session.activateOnCurrentThread()
            session.reload()
            val loaderResult = loader(session, obj)
            println("loader result id:${loaderResult?.identity}/v:${loaderResult?.version} for ${obj}: ${loaderResult}")
            if(loaderResult != null) {
                record = loaderResult
                //this record will be saved later
                if(rootIds.contains(record.identity)) {
                    mySave = false
                }
                rootIds.add(record.identity)
            } else {
                session.activateOnCurrentThread()
                session.begin()
                record =  session.newInstance<ODocument>(obj::class.simpleName).save()
                session.commit()
            }
        }
        obj::class.declaredMemberProperties.forEach { property ->
            handleProperty(session, record, property, obj, rootIds, save)
        }
        record.setProperty(CLASS_HINT, obj::class.jvmName)
        if(mySave)  {
            session.activateOnCurrentThread()
//            session.begin()
            record = session.save(record)
//            session.activateOnCurrentThread()
//            session.commit()
//            record.reload()
            println("Saved  id:${record?.identity}/v:${record?.version} for ${obj}: $record")
        }
        return record
    }

    private fun <T : Any> saveDocument(obj: T, session: ODatabaseSession, rootIds: MutableSet<ORID>, save:Boolean = false): ODocument? {
        var close = false
        var record:ODocument? = null

        for(i in 0..5) {
            try {
                record = createDocument(session, obj,save=save, rootIds = rootIds)
                break
            } catch (e: Exception) {
                when(e) {
                    is ONeedRetryException, is OConcurrentModificationException -> {
                        println("Got exception to retry for $record: $e")
                        Thread.sleep(50)
                    }
                    else -> throw e
                }
            }
        }
        return record
    }

    private fun <T : Any> handleProperty(session: ODatabaseSession, record: ODocument, property: KProperty1<out Any, Any?>, obj: T, rootIds: MutableSet<ORID>, save:Boolean=false) {
        val readProperty = readProperty<Any>(obj, property.name)
        val readPropertyClass = readProperty::class
        if (isSimpleProperty(property)) {
            record.setProperty(property.name, readProperty)
        } else if (isObjectProperty(property)) {
            if (!loaders.containsKey(readPropertyClass.simpleName)) {
                val objRecord = createDocument(session, readProperty, rootIds = rootIds, save = false)
                record.setProperty(property.name, objRecord)
            } else {
                val objRecord = saveDocument(readProperty, rootIds = rootIds, session = session, save=save)
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
                                .map { createDocument(session, it!!, rootIds = rootIds, save = false) }
                        record.setProperty(property.name, values)

                    } else {
                        val values = readProperty<Set<*>>(obj, property.name)
                                .filter { it != null }
                                .map { createDocument(session, it!!, rootIds = rootIds, save = false) }.toSet()
                        record.setProperty(property.name, values)
                    }
                } else {
                    if (property.returnType.isSubtypeOf(List::class.starProjectedType)) {
                        val values = readProperty<List<*>>(obj, property.name)
                                .filter { it != null }
                                .map { saveDocument(it!!, rootIds = rootIds, session = session, save = save) }
                                .map { it?.identity }
                        record.setProperty(property.name, values)

                    } else {
                        val values = readProperty<Set<*>>(obj, property.name)
                                .filter { it != null }
                                .map { saveDocument(it!!, rootIds = rootIds, session = session, save = save) }
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
                            .map { Pair(it.key, createDocument(session, it.value!!, rootIds = rootIds, save = false)) }
                            .toMap()
                    record.setProperty(property.name, values)
                } else {
                    val values = readProperty<Map<*, *>>(obj, property.name)
                            .filter { it.value != null }
                            .map { Pair("${it.key}", saveDocument(it.value!!, rootIds = rootIds, session=session, save=save)?.identity) }
                            .toMap()
                    record.setProperty(property.name, values)
                }
            }
        } else if (!loaders.containsKey(readPropertyClass.simpleName)) {
            //don't know how to retrieve object => saveDocument it serialized

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

        nameToKotlinClass.set(listClass?.simpleName ?: "", listClass as KClass<Any>)

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



