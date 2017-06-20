## Kafka-http ##
To allow user insert message into Kafka topic. You could choose the best image as needs.

## How to run ##

Before build docker image, please compile "go" first.

To compile "kafka-http.go": 
**CGO_ENABLED=0 GOOS=linux go build -o kafka-http .**

To compile "kafka-http-config.go":
**CGO_ENABLED=0 GOOS=linux go build -o kafka-http-config .**


1. [Kafka-http](https://github.com/cutejaneii/kafka-http/tree/master/kafka-http)

The kafka broker list need to be given when execute "docker run" command.

To run it : docker run -d -p 8880:8880 -e KAFKA_PEERS=192.168.0.1:9999 cutejaneii/kafka-http


2. [kafka-http-config](https://github.com/cutejaneii/kafka-http/tree/master/kafka-http-config)

Use "config" file to maintain broker list.

To run it : docker run -d -p 8700:8700 cutejaneii/kafka-http
