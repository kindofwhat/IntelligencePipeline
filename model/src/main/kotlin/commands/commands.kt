package commands

import kotlinx.serialization.Serializable

@Serializable
data class StartPipeline(val bootstrap:String="", val stateDirectory:String="", val scanDirectory:String="")