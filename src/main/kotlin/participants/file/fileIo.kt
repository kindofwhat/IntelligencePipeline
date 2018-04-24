package participants.file

import datatypes.DataRecord
import pipeline.capabilities.*
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Paths



class FileOriginalContentCapability: OriginalContentCapability {
    override fun execute(name: String, dataRecord: DataRecord): InputStream? {
        val file = File(dataRecord.representation.path)
        if (file.isFile && file.canRead()) {
            return file.inputStream()
        }
        return null
    }
}

fun fileRepresentationStrategy(rootPath: String, dataRecord: DataRecord, ending: String, createFileIfNotExisting:Boolean): File? {

    val regex = Regex(":|\\\\|/")
    val file = File(rootPath + "/" + regex.replace(dataRecord.representation.path,"-")+ "." + ending)
    if(!file.exists() && createFileIfNotExisting && !file.isDirectory) {
        Files.createDirectories(Paths.get(file.toURI()).parent)
        return file
    } else if(file.exists()) {
        return file
    }

    return null
}
class FileSimpleTextOutPathCapability(val rootPath: String):SimpleTextOutPathCapability {
    override fun execute(name: String, dataRecord: DataRecord): String? {
        return fileRepresentationStrategy(rootPath,dataRecord,"txt", true)?.path
    }
}

class FileHtmlTextOutPathCapability(val rootPath: String):HtmlTextOutPathCapability {
    override fun execute(name: String, dataRecord: DataRecord): String? {
        return fileRepresentationStrategy(rootPath,dataRecord,"html", true)?.path
    }
}


class FileTxtOutputProvider(val rootPath:String) : TxtTextCapabilityOut {
    override fun execute(name: String, dataRecord: DataRecord): OutputStream? {
        return fileRepresentationStrategy(rootPath,dataRecord,"txt", true)?.outputStream()
    }
}

class FileHtmlOutputProvider(val rootPath:String) : HtmlCapabilityOut {
    override fun execute(name: String, dataRecord: DataRecord): OutputStream? {
        return fileRepresentationStrategy(rootPath,dataRecord,"html", true)?.outputStream()
    }
}

class FileHtmlStringProvider(val rootPath:String) : HtmlTextCapabilityIn {
    override fun execute(name: String, dataRecord: DataRecord): String? {
        return fileRepresentationStrategy(rootPath,dataRecord,"html", false)?.readText(Charsets.UTF_8)
    }
}

class FileTxtStringProvider(val rootPath:String) : TxtTextCapabilityIn {
    override fun execute(name: String, dataRecord: DataRecord): String? {
        return fileRepresentationStrategy(rootPath,dataRecord,"txt", false)?.readText(Charsets.UTF_8)
    }
}
