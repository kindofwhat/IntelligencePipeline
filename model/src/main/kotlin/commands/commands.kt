package commands

data class StartPipeline(val bootstrap:String="", val stateDirectory:String="", val scanDirectory:String="")