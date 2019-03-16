package participants.file

import pipeline.capabilities.*
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Paths



@HasCapabilities(originalContentIn) class FileOriginalContentCapability: Capability<InputStream?> {
    override fun execute(name: String, dataRecord: datatypes.DataRecord): InputStream? {
        val file = File(dataRecord.representation.path)
        if (file.isFile && file.canRead()) {
            return file.inputStream()
        }
        return null
    }
}

fun fileRepresentationStrategy(rootPath: String, dataRecord: datatypes.DataRecord, ending: String, createFileIfNotExisting:Boolean): File? {

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

@HasCapabilities(simpleTextOutPath) class FileSimpleTextOutPathCapability(val rootPath: String):Capability<String?> {
    override fun execute(name: String, dataRecord: datatypes.DataRecord): String? {
        return fileRepresentationStrategy(rootPath,dataRecord,"txt", true)?.path
    }
}

@HasCapabilities(htmlTextOutPath) class FileHtmlTextOutPathCapability(val rootPath: String):Capability<String?> {
    override fun execute(name: String, dataRecord: datatypes.DataRecord): String? {
        return fileRepresentationStrategy(rootPath,dataRecord,"html", true)?.path
    }
}


@HasCapabilities(simpleTextOut)class FileTxtOutputProvider(val rootPath:String) : Capability<OutputStream?> {
    override fun execute(name: String, dataRecord: datatypes.DataRecord): OutputStream? {
        return fileRepresentationStrategy(rootPath,dataRecord,"txt", true)?.outputStream()
    }
}

@HasCapabilities(htmlTextOut) class FileHtmlOutputProvider(val rootPath:String) : Capability<OutputStream?> {
    override fun execute(name: String, dataRecord: datatypes.DataRecord): OutputStream? {
        return fileRepresentationStrategy(rootPath,dataRecord,"html", true)?.outputStream()
    }
}

@HasCapabilities(htmlTextIn) class FileHtmlStringProvider(val rootPath:String) : Capability<String?> {
    override fun execute(name: String, dataRecord: datatypes.DataRecord): String? {
        return fileRepresentationStrategy(rootPath,dataRecord,"html", false)?.readText(Charsets.UTF_8)
    }
}

@HasCapabilities(simpleTextIn) class FileTxtStringProvider(val rootPath:String) :  Capability<String?> {
    override fun execute(name: String, dataRecord: datatypes.DataRecord): String? {
        return fileRepresentationStrategy(rootPath,dataRecord,"txt", false)?.readText(Charsets.UTF_8)
    }
}
