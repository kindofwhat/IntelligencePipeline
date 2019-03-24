package unittests

import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertTrue
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.Serializable
import org.junit.Test
import orientdb.createReflectedSchema
import orientdb.persist
import orientdb.schemaPersist


@Serializable
data class A(val a: String, val b: List<B>, val c: Long, val d: List<String>)

@Serializable
data class B(val a: A, val b: String, val c: List<Boolean>)


@ImplicitReflectionSerializer
class OrientDBFunctionsTest {
    @Test
    fun testSchemaCreation() {
        val dbName = "testSchemaCreation"
//        val db = OrientDB("memory", OrientDBConfig.defaultConfig())
        val db = OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig())

        if(db.exists(dbName)) db.drop(dbName)
        db.create(dbName, ODatabaseType.MEMORY)
        val session = db.open(dbName, "admin", "admin")
        createReflectedSchema(session, A::class)
        session.commit()


        val schema = session.metadata.schema
        assertTrue(schema.existsClass("A"))
        assertEquals(4, schema.getClass("A").properties().size)
        assertTrue(schema.existsClass("B"))
        assertEquals(3, schema.getClass("B").properties().size)
    }


    @Test fun testPersistAndReload() {
        val dbName = "testPersistAndReload"
//        val db = OrientDB("memory", OrientDBConfig.defaultConfig())
        val db = OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig())

        if(db.exists(dbName)) db.drop(dbName)
        db.create(dbName, ODatabaseType.MEMORY)
        val session = db.open(dbName, "admin", "admin")
        createReflectedSchema(session, A::class)
        session.commit()

        val a = A(a="AAA", b = listOf(), c = 1L, d = listOf("d1", "d2"))
        val b = B(a=a, b="BBB", c = listOf(false,true))
        val b2 = B(a=a, b="B2", c = listOf(true,false))
        val a2 =  A(a="A2", b = listOf(b,b2), c = 2L, d = listOf("d21", "d22"))

        schemaPersist(session,a)
        schemaPersist(session,b)
        schemaPersist(session,a2)

    }
}
