# Intelligence Pipeline - Goals
* provide a fast and persistent data pipeline for documents
* be as open as possible for modern data analytics/machine learning/AI frameworks
    * make use of those frameworks, do not try to reproduce them
    * make them comparable by storing the results in a comparable format
* gather as much information as possible about documents
    * Metadata
    * Authorization
    * inner structure
    * relations
    * categories

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
## Notes
- make sure to set advertised.host.name in the bootstrap config


# Open questions

## logic of changes to source data and enrichment
* Use case 1: Two Metadata-Producer produce independent and maybe conflicting data ==> there has to be a possibility to 
know about those conflicting results and act accordingly (e.g. choose one, or an average)
* Use case 2: To detect sentence boundary, a ChunkProducer makes an educated guess. 
Then a human makes some saner choices. Those choices are kept when the document content changes.
* Use case 3: a quite dump ChunkProducer makes a first pass. Then a better system optimized upon it. This might be usefull 
for systems where the second, better producer cannot work efficiently with large documents (e.g. StanfordNLP) 

### Solution outlines
*  give each producer a weight: the higher the more important it is

## MISC
* why does exactly once not work?
* how to handle deletes?


# Roadmap
## V0.1: make it available on Github

### Open
- [ ]  Good test coverage


### Done
- [x] robust exception handling: test with a rogue MetadataProducer
- [x] Quickstart Guide
- [x] Finer Domain Model: Sentences, Token, etc
- [x] Participiants, Capabilities and requirements
- [x] check that it's running with local cluster
- [x] Keep represenation off kafka (simple file handling)
- [x] make it available on Github
- [x] Basic result reporting

## V0.2: Check all use cases with Kafka

### Open
- [ ] uberjar or something similar
- [ ] simple regex based keyword extraction
- [ ] make it available online somewhere
- [ ] Multiple IP in parallel
- [ ] Ingestion is on schedule
- [ ] Large file strategy: many metadata producers take too long, e.g. stanfordnlp => tokenize this
- [ ] IP can be rescheduled
- [ ] Intelligent reprocessing of Metadata
- [ ] Basic GUI
- [ ] coroutines for MetadataProducers and where it makes sense


### Done


## V0.3: feature complete

### Open
- [ ] Possibility for external process to attach via batch
- [ ] more Ingestors
- [ ] Ingestors based on Kafka Connect
- [ ] One ouptclient (DBMS or Graph DB) based on Kafka Connect
- [ ] (optional) REST API


### Done

## V0.4: let them fight
Have different frameworks compete and have means to select/evaluate the best possibility

### Open


### Done


#Design Considerations
## Domain Design: Inner structure of documents
How can the inner structure (i.e. sections, paragraphs, sentences, words, tokens) be designed in a streaming enivonrment?

==> at the moment: Chunks

==> do not keep the whole text within kafka as one block, but use messages to describe the structure (e.g. "ADD SENTENCE")

