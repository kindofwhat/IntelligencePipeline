/*
 * Gson: https://github.com/google/gson
 * Maven info:
 *     groupId: com.google.code.gson
 *     artifactId: gson
 *     version: 2.8.1
 *
 * Once you have compiled or downloaded gson-2.8.1.jar, assuming you have placed it in the
 * same folder as this file (keyPhrases.java), you can compile and run this program at
 * the command line as follows.
 *
 * javac keyPhrases.java -classpath .;gson-2.8.1.jar -encoding UTF-8
 * java -cp .;gson-2.8.1.jar keyPhrases
 */
import com.github.kittinunf.fuel.httpPost
import kotlinx.serialization.ImplicitReflectionSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JSON
import kotlinx.serialization.parse
import kotlinx.serialization.stringify

@Serializable
data class RequestDocument(val id: String, val language:String, val text: String)

@Serializable
data class RequestDocuments(val documents: List<RequestDocument>)

@Serializable
data class ResponseDocument(val id: String, val keyPhrases: List<String> = arrayListOf())

@Serializable
data class ResponseDocuments(val documents:List<ResponseDocument>, val errors: List<String> = arrayListOf())



@ImplicitReflectionSerializer
object GetKeyPhrases {

    internal var host = "https://westus.api.cognitive.microsoft.com"
    internal var path = "/text/analytics/v2.0/keyPhrases"

    @Throws(Exception::class)
    fun keyPhrases(documents: RequestDocuments): String {
        val text = JSON.stringify(documents)
        val encoded_text = text.toByteArray(charset("UTF-8"))
        val url = host + path
        return url.httpPost()
                .header(hashMapOf( "Content-Type" to "text/json", "Ocp-Apim-Subscription-Key" to System.getProperty("accessKey")))
                .body(encoded_text)
                .responseString(Charsets.UTF_8).third.component1()?:""
    }


    @JvmStatic
    fun main(args: Array<String>) {
        try {
            val documents = RequestDocuments(listOf(
            RequestDocument("1", "en", "I really enjoy the new XBox One S. It has a clean look, it has 4K/HDR resolution and it is affordable."),
                    RequestDocument("2", "es", "Si usted quiere comunicarse con Carlos, usted debe de llamarlo a su telefono movil. Carlos es muy responsable, pero necesita recibir una notificacion si hay algun problema."),
                    RequestDocument("3", "en", "The Grand Hotel is a new hotel in the center of Seattle. It earned 5 stars in my review, and has the classiest decor I've ever seen.")))

            val response = keyPhrases(documents)
            println(response)
            val result = JSON.parse<ResponseDocuments>(response)
            println(result)
        } catch (e: Exception) {
            println(e)
        }

    }
}