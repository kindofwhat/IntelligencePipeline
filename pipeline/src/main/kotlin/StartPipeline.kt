import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import pipeline.IIntelligencePipeline
import pipeline.impl.KafkaIntelligencePipeline
import pipeline.impl.MapIntelligencePipeline
import kotlin.system.exitProcess


fun main(args: Array<String>) {
    var pipeline: IIntelligencePipeline? = null
    val usage = "Usage <type> <kafka-bootstrap> where <type>=m or k (m: in memory, k: kafka) " +
            "and <kafka-bootstrap>=kafka bootstrap url, e.g. 'localhost:9092'"
    if (args.size < 1) {
        println(usage)
        exitProcess(1);
    }
    if (args.get(0).toLowerCase().equals('m')) {
        pipeline = MapIntelligencePipeline()
    } else if (args.get(0).toLowerCase().equals('m')) {
        if (args.size < 2) {
            println(usage)
            exitProcess(1)
        }
        pipeline = KafkaIntelligencePipeline(args.get(1), "state")
    }
    val job = launch {
        println("starting pipeline, enter anything to stop")
        pipeline?.run()
    }
    fun stop() {
        println("stopping pipeline")
        pipeline?.stop()
        println("stopped pipeline")
        job.cancel()

    }
    Runtime.getRuntime().addShutdownHook(object : Thread() {
        override fun run() {
            stop()
        }
    })
    while (true) {
        if(readLine()?.length!! >0) {
            exitProcess(0)
        }
        Thread.sleep(1000);
    }
}
