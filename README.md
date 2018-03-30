# Intelligence Pipeline - Goals
* provide a fast and persistent data pipeline for documents
* be as open as possible for modern data analytics/machine learning/AI frameworks
* gather as much information as possible of documents
    * Metadata
    * Authorization
    * inner structure
    * relations
    * categories
* have a way to compare and weight the results of the different frameworks, e.g. LanuageDetection by Apache Tika and Google


# Path chosen
* be open source
* use kotlin
* use kotlin coroutines for modern development
* data is streaming data, 't is a fact
* rely on Apache Kafka, 't is a beast
* have a well designed solution domain

# Quickstart
1. Start a Kafka Cluster
2. Initialise an IntelligencePipeline
```kotlin
val pipeline = IntelligencePipeline(bootstrapServers, stateDir)
```
where `bootstrapServers` is something like `localhost:9092` and `stateDir` should point to a local directory (used by state stores)
3. register your ingestors:
```kotlin 
pipeline?.registerIngestor(DirectoryIngestor("src/test/resources"))
``` 
4. register your MetaDataProducers:
``` kotlin 
    pipeline?.registerMetadataProducer(TikaMetadataProducer())
    pipeline?.registerMetadataProducer(HashMetadataProducer())
``` 
5. start the pipeline
``` kotlin 
    launch {
        pipeline?.run()
    }
``` 


# Open questions

## Domain Design: Inner structure of documents
How can the inner structure (i.e. sections, paragraphs, sentences, words, tokens) be designed in a streaming enivonrment?

# Notes
- make sure to set advertised.host.name in the bootstrap config

# Roadmap
## V0.1: make it available on Github

### Open
- [ ]  Finer Domain Model
- [ ]  Quickstart Guide
- [ ]  Good test coverage
- [ ]  robust exception handling: test with a rogue MetadataProducer
- [ ]  Basic result reporting
- [ ]  make it available online somewhere

### Done
- [x] Participiants, Capabilities and requirements
- [x] check that it's running with local cluster
- [x] Keep represenation off kafka (simple file handling)

## V0.2: Check all use cases with Kafka

### Open
- [ ] Multiple IP in parallel
- [ ] IP can be rescheduled
- [ ] Intelligent reprocessing of Metadata
- [ ] Basic GUI
- [ ] coroutines for MetadataProducers


### Done


## V0.3: feature complete

### Open
- [ ] Possibility for external process to attach via batch
- [ ] more Ingestors
- [ ] Ingestors based on Kafka Connect
- [ ] One ouptclient (DBMS or Graph DB) based on Kafka Connect
- [ ] (optional) REST API


### Done
