#!/usr/bin/env bash

#curl -X post --data '{"bootstrap":"localhost:29092","stateDirectory":"/tmp","scanDirectory":"/home/christian/Dokumente/workspace/IntelligencePipeline/pipeline/src/test/resources/testresources/"}' localhost:7000/startPipeline
curl -X post --data '{"bootstrap":"localhost:29092","stateDirectory":"/tmp","scanDirectory":"/home/christian/Dokumente/osw/"}' localhost:7000/startPipeline
