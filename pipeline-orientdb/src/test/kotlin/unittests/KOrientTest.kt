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
import org.junit.Test
import orientdb.FieldLoader
import orientdb.Loader
import orientdb.KOrient





data class A(val a: String = "", val b: List<B> = emptyList(), val c: Long = -1L, val d: List<String> = emptyList())
data class B(val a: A = A(), val b: String = "", val c: List<Boolean> = emptyList())
data class C(val a: A = A(), val b: B = B(), val m1: Map<String,B> = emptyMap(), val m2:Map<String, Int> = emptyMap(), val s: Set<B> = emptySet())
data class D(val id:String = "", val c: C = C(), val cset: Set<C> = emptySet(), val cmap: Map<String, C> = emptyMap(), val clist: List<C> = emptyList())

open class P(open val id:String, open val a:A)
data class C1(override val id: String, override val a:A, val b:B):P(id,a)
data class C2(override val id:String, override val a:A, val c:C):P(id,a)
data class R(val id:String, val p:P)

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


    @Test
    fun testHierarchicalSchemaCreation() {
        val dbName = "testHierarchicalSchemaCreation"
//        val db = OrientDB("memory", OrientDBConfig.defaultConfig())
        val db = OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig())

        if(db.exists(dbName)) db.drop(dbName)
        db.create(dbName, ODatabaseType.MEMORY)
        //don't have to create session for B since it is depending on A
        val loaders = mutableMapOf<String, Loader<Any>>()
        loaders.put("A", FieldLoader("a"))
        loaders.put("B", FieldLoader("b"))
        loaders.put("C", FieldLoader("id"))
        loaders.put("P", FieldLoader("id"))
        loaders.put("C1", FieldLoader("id"))
        loaders.put("C2", FieldLoader("id"))
        val korient = KOrient("remote:localhost", dbName,"root", "root",loaders)

        korient.createSchema(A::class)
        korient.createSchema(B::class)
        korient.createSchema(C::class)
        korient.createSchema(C1::class)
        korient.createSchema(C2::class)


        val session = db.open(dbName, "admin", "admin")
        val schema = session.metadata.schema
        assertTrue(schema.existsClass("A"))
        assertEquals(4, schema.getClass("A").properties().size)
        assertTrue(schema.existsClass("B"))
        assertEquals(3, schema.getClass("B").properties().size)


        assertTrue(schema.existsClass("C"))

        assertTrue(schema.existsClass("P"))
        assertTrue(schema.existsClass("C1"))
        assertTrue(schema.existsClass("C2"))




        val c1Schema = schema.getClass("C1")
        assertEquals(3, c1Schema.properties().size)

        c1Schema.allSuperClasses.contains(schema.getClass("P"))
    }


    @Test
    fun testHiearchicalReloading() {
        val dbName = "testHiearchicalReloading"
//        val db = OrientDB("memory", OrientDBConfig.defaultConfig())
        val db = OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig())

        if(db.exists(dbName)) db.drop(dbName)
        db.create(dbName, ODatabaseType.MEMORY)
        //don't have to create session for B since it is depending on A
        val loaders = mutableMapOf<String, Loader<Any>>()
        loaders.put("P", FieldLoader("id"))
        loaders.put("C1", FieldLoader("id"))
        loaders.put("C2", FieldLoader("id"))
        loaders.put("R", FieldLoader("id"))

        val korient = KOrient("remote:localhost", dbName,"root", "root",loaders)

        korient.createSchema(C1::class)
        korient.createSchema(C2::class)
        korient.createSchema(R::class)

        val session = db.open(dbName, "admin", "admin")
        val schema = session.metadata.schema

        assertTrue(schema.existsClass("P"))
        assertTrue(schema.existsClass("C1"))
        assertTrue(schema.existsClass("C2"))
        assertTrue(schema.existsClass("R"))

        val a = A(a="a")
        val b = B(b="b")
        val c = C(a=a, b=b)
        val c1 = C1(id="c1", a = a, b = b)
        val c2 = C2(id="c2", a = a, c = c)

        val r1= R(id="r1", p = c1)
        val r2= R(id="r2", p = c2)

        with(korient) {
            save(a)
            save(b)
            save(c)
            save(c1)
            save(c2)
            save(r1)
            save(r2)
        }
        runBlocking {
            val aRes = korient.queryAll(R::class).toList()
            assertEquals(2, aRes.size)
            assertEquals(r1,aRes.get(0))
            assertEquals(r2,aRes.get(1))
        }


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

        korient.save(a)
        korient.save(b)
        korient.save(a2)
        korient.save(c)


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
        korient.createSchema( C::class)
        korient.createSchema( D::class)

        val a = A(a="AAA", b = listOf(), c = 1L, d = listOf("d1", "d2"))
        val b = B(a=a, b="BBB", c = listOf(false,true))
        val b2 = B(a=a, b="B2", c = listOf(true,false))
        val a2 =  A(a="A2", b = listOf(b,b2), c = 2L, d = listOf("d21", "d22"))

        val c = C(a=a, b=b, m1=  mapOf("a2" to b2), m2 = mapOf("one" to 1, "two" to 2), s = setOf(b))

        val d = D("123", c, setOf(c), mapOf("1" to c), listOf(c))

        korient.save(a)
        korient.save(b)
        korient.save(a2)
        korient.save(d)
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
