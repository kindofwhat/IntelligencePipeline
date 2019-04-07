package unittests

import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import junit.framework.Assert.*
import kotlinx.coroutines.channels.toList
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.Serializable
import org.junit.Test
import orientdb.FieldLoader
import orientdb.Loader
import orientdb.KOrient


@Serializable
data class A(val a: String = "", val b: List<B> = emptyList(), val c: Long = -1L, val d: List<String> = emptyList())

@Serializable
data class B(val a: A = A(), val b: String = "", val c: List<Boolean> = emptyList())


@Serializable
data class C(val a: A = A(), val b: B = B(), val m1: Map<String,B> = emptyMap(), val m2:Map<String, Int> = emptyMap(), val s: Set<B> = emptySet())

@Serializable
data class D(val id:String = "", val c: C = C(), val cset: Set<C> = emptySet(), val cmap: Map<String, C> = emptyMap(), val clist: List<C> = emptyList())


@ImplicitReflectionSerializer
class KOrientTest {
    @Test
    fun testSchemaCreation() {
        val dbName = "testSchemaCreation"
//        val db = OrientDB("memory", OrientDBConfig.defaultConfig())
        val db = OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig())

        if(db.exists(dbName)) db.drop(dbName)
        db.create(dbName, ODatabaseType.MEMORY)
        //don't have to create session for B since it is depending on A
        val loaders = mutableMapOf<String, Loader<Any>>()
        loaders.put("A", FieldLoader("a"))
        loaders.put("B", FieldLoader("b"))
        loaders.put("D", FieldLoader("id"))
        val korient = KOrient("remote:localhost", dbName,"root", "root",loaders)

        korient.createSchema(A::class)
        korient.createSchema(D::class)


        val session = db.open(dbName, "admin", "admin")
        val schema = session.metadata.schema
        assertTrue(schema.existsClass("A"))
        assertEquals(4, schema.getClass("A").properties().size)
        assertTrue(schema.existsClass("B"))
        assertEquals(3, schema.getClass("B").properties().size)


        assertFalse(schema.existsClass("C"))
        assertTrue(schema.existsClass("D"))
        val cSchema = schema.getClass("D")
        assertEquals(5, cSchema.properties().size)

        assertEquals(OType.EMBEDDED,cSchema.propertiesMap().get("c")?.type)
        assertEquals(OType.EMBEDDEDSET,cSchema.propertiesMap().get("cset")?.type)
        assertEquals(OType.EMBEDDEDMAP,cSchema.propertiesMap().get("cmap")?.type)
        assertEquals(OType.EMBEDDEDLIST,cSchema.propertiesMap().get("clist")?.type)


    }


    @Test fun testPersistAndReload() {
        val dbName = "testPersistAndReload"
        val db = OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig())

        if(db.exists(dbName)) db.drop(dbName)
        db.create(dbName, ODatabaseType.MEMORY)

        val loaders = mutableMapOf<String, Loader<Any>>()

        //A, B and C are leading classes
        loaders.put("A", FieldLoader("a"))
        loaders.put("B", FieldLoader("b"))
        loaders.put("C")  { session,  c ->
            var res:ODocument? = null
            val cc = c as C
            val result = session.query("SELECT FROM C WHERE a.a = '${cc.a.a}'  and b.b = '${cc.b.b}'")
            if (result.hasNext()) {
                val ores = result.next()
                val existingDocument = ores.toElement() as ODocument
                result.close()
                res = existingDocument
            }
            res
        }


        val korient = KOrient("remote:localhost", dbName,"root", "root",loaders)
        korient.createSchema(A::class)
        korient.createSchema(C::class)

        val a = A(a="AAA", b = listOf(), c = 1L, d = listOf("d1", "d2"))
        val b = B(a=a, b="BBB", c = listOf(false,true))
        val b2 = B(a=a, b="B2", c = listOf(true,false))
        val a2 =  A(a="A2", b = listOf(b,b2), c = 2L, d = listOf("d21", "d22"))

        val c = C(a=a, b=b, m1=  mapOf("a2" to b2), m2 = mapOf("one" to 1, "two" to 2), s = setOf(b))

        korient.saveDocument(a)
        korient.saveDocument(b)
        korient.saveDocument(a2)
        korient.saveDocument(c)


        runBlocking {
            val aRes = korient.queryAll(A::class).toList()
            assertEquals(2, aRes.size)
            assertEquals(a,aRes.get(0))
            assertEquals(a2,aRes.get(1))
            //b2 is saved with a
            val cRes = korient.queryAll(C::class).toList()
            assertEquals(1, cRes.size)
            assertEquals(c,cRes.get(0))
        }

    }


    @Test
     fun testPersistAndReloadWithInnerClasses() {
        val dbName = "testPersistAndReloadWithInnerClasses"
        val db = OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig())

        if(db.exists(dbName)) db.drop(dbName)
        db.create(dbName, ODatabaseType.MEMORY)

        val loaders = mutableMapOf<String, Loader<Any>>()
        loaders.put("A", FieldLoader("a"))
        loaders.put("B", FieldLoader("b"))
        loaders.put("D", FieldLoader("id"))

        val korient = KOrient("remote:localhost", dbName,"root", "root",loaders)
        korient.createSchema( A::class)
        korient.createSchema( B::class)
        korient.createSchema( D::class)

        val a = A(a="AAA", b = listOf(), c = 1L, d = listOf("d1", "d2"))
        val b = B(a=a, b="BBB", c = listOf(false,true))
        val b2 = B(a=a, b="B2", c = listOf(true,false))
        val a2 =  A(a="A2", b = listOf(b,b2), c = 2L, d = listOf("d21", "d22"))

        val c = C(a=a, b=b, m1=  mapOf("a2" to b2), m2 = mapOf("one" to 1, "two" to 2), s = setOf(b))

        val d = D("123", c, setOf(c), mapOf("1" to c), listOf(c))

        korient.saveDocument(a)
        korient.saveDocument(b)
        korient.saveDocument(a2)
        korient.saveDocument(d)
        runBlocking {
            val aRes = korient.queryAll(A::class).toList()
            assertEquals(2, aRes.size)
            assertEquals(a,aRes.get(0))
            assertEquals(a2,aRes.get(1))
            //b2 is saved with a
            val bRes = korient.queryAll(B::class).toList()
            assertEquals(2, bRes.size)
            assertEquals(b,bRes.get(0))
            assertEquals(b2,bRes.get(1))
            val dRes = korient.queryAll(D::class).toList()
            assertEquals(1, dRes.size)
            assertEquals(d,dRes.get(0))
        }
    }
}
