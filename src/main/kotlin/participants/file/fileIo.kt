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
    val file = File(rootPath + "/" + dataRecord.hashCode() + "." + ending)
    if(!file.exists() && createFileIfNotExisting) {
        Files.createDirectories(Paths.get(file.toURI()).parent)
        return file
    } else if(file.exists()) {
        return file
    }

    return null
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
