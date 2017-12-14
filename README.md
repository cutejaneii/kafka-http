## Kafka-http ##
To allow user insert message into Kafka topic. You could choose the best image as needs.

## How to run ##

#### Step 1. Compile "go" first. ####

To compile "kafka-http.go": 
**CGO_ENABLED=0 GOOS=linux go build -o kafka-http .**

To compile "kafka-http-config.go":
**CGO_ENABLED=0 GOOS=linux go build -o kafka-http-config .**


#### Step 2. You can execute directly after compile. ####

./kafka-http --brokers=192.168.0.1:9092

./kafka-http-config


#### Step 3. docker build to get the docker image. ####


#### Step 4. docker run. ####

1. [Kafka-http](https://github.com/cutejaneii/kafka-http/tree/master/kafka-http)

The kafka broker list need to be given when execute "docker run" command.

`docker run -d -p 8880:8880 -e KAFKA_PEERS=192.168.0.1:9092 cutejaneii/kafka-http`


2. [kafka-http-config](https://github.com/cutejaneii/kafka-http/tree/master/kafka-http-config)

Use "config" file to maintain broker list.

`docker run -d -p 8700:8700 cutejaneii/kafka-http`

#### Step 5. How to call API. ####
Method : POST
Add Header: Content-Type, application/json
input:
`{`
`"topic":"topic_name",`
`"value":"hello, world!"`
`}`
