package test.com.example

import kotlin.test.Test
import kotlin.test.assertTrue

class AppSpec : DomSpec {

    @Test
    fun render() {
        run {
            assertTrue(true, "Dummy test")
        }
    }
}
