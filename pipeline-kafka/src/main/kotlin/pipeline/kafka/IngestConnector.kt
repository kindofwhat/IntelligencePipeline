package pipeline.kafka

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.data.Schema.STRING_SCHEMA
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import util.log
import java.io.File
import java.time.Period


data class Config(val path:String, val topic: String, val period:Period) {
    enum class CONFIG { PATH, TOPIC, PERIOD}

    companion object {
        fun fromConfig(props: MutableMap<String, String>?): Config {
            val path = props?.get(CONFIG.PATH.toString())?:""
            val topic = props?.get(CONFIG.TOPIC.toString())?:""
            val periodString = props?.get(CONFIG.PERIOD.toString())?:""
            var period: Period
            try{
                period = Period.parse(periodString)
            }catch (e:Exception) {
                TODO("correct logging")
            }
            return Config(path, topic, period)
        }
    }
}

class IngestConnector: SourceConnector() {

    private var config: Config = Config("", "", Period.ZERO)

    override fun taskConfigs(maxTasks: Int): MutableList<MutableMap<String, String>> {
        val oneConfig = mutableMapOf(
                Config.CONFIG.PATH.toString() to config.path,
                Config.CONFIG.TOPIC.toString() to config.topic,
                Config.CONFIG.PERIOD.toString() to config.period.toString())
        return mutableListOf(oneConfig)
    }



    override fun start(props: MutableMap<String, String>?) {
        config = Config.fromConfig(props)
    }

    override fun stop() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun version(): String {
        return "0.0.1"
    }

    override fun taskClass(): Class<out Task> {
       return IngestTask::class.java
    }

    override fun config(): ConfigDef {
        val config = ConfigDef()
        config.define(Config.CONFIG.PATH.toString(),ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,"just a description")
        return config
    }
}

class IngestTask: SourceTask() {
    private var config: Config = Config("", "", Period.ZERO)

    override fun start(props: MutableMap<String, String>?) {
        config = Config.fromConfig(props)
    }

    override fun stop() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun version(): String {
        return "0.0.1"
    }

    override fun poll(): MutableList<SourceRecord> {
        var result = mutableListOf<SourceRecord>()
        File(config.path).walkTopDown().forEachIndexed { idx, file ->
            log("ingest " + file.absolutePath)

            val sourcePartition = mapOf<String,String>("filename" to file.absolutePath)
            val sourceOffset = mapOf<String,String>("position" to  ""+ idx)
            result.add(SourceRecord(sourcePartition, sourceOffset, config.topic, STRING_SCHEMA, file.absolutePath))
        }
        return result
    }

}