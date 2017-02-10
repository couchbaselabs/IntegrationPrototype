# IntegrationPrototype

This code need to be complied along with GOXDCR code to get it working.

Docs Folder : This folder contains Nats pub sub  
   (nats.io/documentation/streaming/nats-streaming-intro/) evaluation along with performance metrics like throughput and latency along diffrent workload.

parts/messaging_nozzle.go : New Messaging Nozzle to publish dcp mutation.

tests/messaging/messaging_run.go : Unit test to test messaging nozzle

client/sub_es_client.go : Elastic search subscriber ( THis contain code that will listen to dcp mutation published by messaging nozzle and than it will send them to elastic search).


To Test

1. Compile this code along with GOXDCR code.
2. Start couchbase server.
3. Start Nats Streaming Server (This can be downloaded from  http://nats.io/download/nats-io/nats-streaming-server/). ( Please note as this is prototype so this step is required eventaully starting nats streaming would become part of goXDCR to messaging lifecycle of natstreaming. Natsstreaming is very light weight and embdedable pub sub. Please look at nats evaluation unders docs folder for complete feature details).
3. In command shell : run messaging_run.go : This should start xdcr pipeline with messaging nozzle on.
4. Run Elastic search cluster.
5. run client/sub_es_client.go 


