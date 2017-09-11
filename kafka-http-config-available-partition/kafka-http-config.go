package main

import (
        "bufio"
        "encoding/json"
        "flag"
        "fmt"
        "log"
        "math/rand"
        "net/http"
        "os"
        "strings"
        "time"

        "github.com/Shopify/sarama"
)

var (
        addr = flag.String("addr", ":8880", "The address to bind to")
        // switch to false if do NOT need to log
        // switch to true to log
        verbose    = flag.Bool("verbose", false, "Turn on Sarama logging")
        brokerList []string
)

func main() {
        flag.Parse()

        // Log
        if *verbose {
                sarama.Logger = log.New(os.Stdout, "[kafka-http] ", log.LstdFlags)
        }

        // Read "config" file to get the broker list & print it
        brokerList := readConfig()

        log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

        // New Server
        server := new(Server)
        server.brokerList = brokerList

        defer func() {
                if err := server.Close(); err != nil {
                        log.Println("Failed to close server", err)
                }
        }()

        log.Fatal(server.Run(*addr))
}

type Server struct {
        DataCollector sarama.SyncProducer
        brokerList    []string
}

// declare object : KafkaInfo
// 定義傳入的資料格式
type KafkaInfo struct {
        Topic string `json:topic`
        Value string `json:value`
}

func (s *Server) Close() error {
        if err := s.DataCollector.Close(); err != nil {
                log.Println("Failed to shut down data collector cleanly", err)
        }

        return nil
}

func (s *Server) Handler() http.Handler {
        return s.collectQueryStringData()
}

func (s *Server) Run(addr string) error {
        httpServer := &http.Server{
                Addr:    addr,
                Handler: s.Handler(),
        }

        log.Printf("Listening for requests on %s...\n", addr)
        return httpServer.ListenAndServe()
}

//func GetAvailableBrokers(client *sarama.Client) {
//      clientActiveBrokers := client.Brokers()
//      for i := range clientActiveBrokers {
//              BrokerID := strconv.Itoa(int(clientActiveBrokers[i].ID()))
//              fmt.Print("\n clientActiveBrokers("+BrokerID+"): ", clientActiveBrokers[i].Addr())
//      }
//}

func KafkaWork(r *http.Request, topic string, topicvalue string) (int32, int64, error) {

        fmt.Print("ABCABCD")

        fmt.Print(brokerList)

        fmt.Print(len(brokerList))
        fmt.Print("\n" + topic)

        // Add sarama config
        config := sarama.NewConfig()
        config.Producer.Return.Successes = true
        config.Producer.Partitioner = sarama.NewManualPartitioner
        fmt.Print("\n" + topic)

        client, err2 := sarama.NewClient(brokerList, config)

        WritablePartitions, err3 := client.WritablePartitions(topic)

        fmt.Print(WritablePartitions[0])

        client.RefreshMetadata(topic)

        for i := range WritablePartitions {
                fmt.Print("\n WritablePartitions: ", WritablePartitions[i])
        }

        if err2 != nil {
                log.Printf("Error... " + err2.Error())
        }

        if err3 != nil {
                log.Printf("Error... " + err3.Error())
        }

        producer2, err5 := sarama.NewSyncProducerFromClient(client)

        if err5 != nil {
                log.Printf("Error... " + err5.Error())
        }

        // We are not setting a message key, which means that all messages will
        // be distributed randomly over the different partitions.
        // 同步傳遞訊息
        partition, offset, err := producer2.SendMessage(&sarama.ProducerMessage{
                Partition: 3, //WritablePartitions[index],
                Topic:     topic,
                Value:     sarama.StringEncoder(topicvalue),
        })

        if err != nil {
                return 0, 0, err
        }

        producer2.Close()
        client.Close()

        return partition, offset, err
}

// Handle everytime when any get any request
func (s *Server) collectQueryStringData() http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

                if r.URL.Path != "/" {
                        http.NotFound(w, r)
                        return
                }

                // Return "Not Found" if user call this api with GET METHOD
                // 如果前端用GET方法呼叫，直接回傳 Not Found
                if r.Method == "GET" {
                        log.Printf("GET method is not allow......")
                        http.NotFound(w, r)
                        return
                }

                // Parse input parameter as KafkaInfo object
                // 把資料轉成KafkaInfo物件
                var kafkaInfo KafkaInfo
                decoderErr := json.NewDecoder(r.Body).Decode(&kafkaInfo)

                // Return "Error" if error when parsing data
                // 解析發生錯誤
                if decoderErr != nil {
                        http.Error(w, decoderErr.Error(), 400)
                        return
                }

                // Add sarama config
                config := sarama.NewConfig()
                config.Producer.Return.Successes = true
                config.Producer.Partitioner = sarama.NewManualPartitioner

                client, err := sarama.NewClient(s.brokerList, config)

                if err != nil {
                        http.Error(w, err.Error(), 500)
                        return
                }

                WritablePartitions, err3 := client.WritablePartitions(kafkaInfo.Topic)

                client.RefreshMetadata(kafkaInfo.Topic)

                for i := range WritablePartitions {
                        fmt.Print("\n WritablePartitions: ", WritablePartitions[i])
                }

                if err3 != nil {
                        log.Printf("Error... " + err3.Error())
                }

                producer2, err := sarama.NewSyncProducerFromClient(client)

                if err != nil {
                        http.Error(w, err.Error(), 500)
                        return
                }

                index := rand.Intn(len(WritablePartitions)) // rand.Intn(5) : 0 <= partitionID < 5

                fmt.Print("\n random index: ", index)
                fmt.Println("\n partition ID: ", WritablePartitions[index])

                // We are not setting a message key, which means that all messages will
                // be distributed randomly over the different partitions.
                // 同步傳遞訊息
                partition, offset, err := producer2.SendMessage(&sarama.ProducerMessage{
                        Partition: WritablePartitions[index], //WritablePartitions[index],
                        Topic:     kafkaInfo.Topic,
                        Value:     sarama.StringEncoder(kafkaInfo.Value),
                })

                producer2.Close()
                client.Close()

                //partition, offset, err := KafkaWork(r, kafkaInfo.Topic, kafkaInfo.Value)

                // Return result
                if err != nil {
                        log.Printf("Error... " + err.Error())

                        w.WriteHeader(http.StatusInternalServerError)
                        //w.Write([]byte(err.Error()))
                        fmt.Fprintf(w, "Failed to store your data, %s", err)
                } else {
                        log.Printf("Success to store one message into [%s], partition:%d, offset:%d", kafkaInfo.Topic, partition, offset)
                        // The tuple (topic, partition, offset) can be used as a unique identifier
                        // for a message in a Kafka cluster
                        w.WriteHeader(http.StatusOK)
                        fmt.Fprintf(w, "Success to store one message into [%s], partition:%d, offset:%d", kafkaInfo.Topic, partition, offset)
                }
        })
}

func readConfig() []string {
        inputFile, err := os.Open("config")
        if err != nil {
                fmt.Println("Open config error!")
                //return
        }

        defer inputFile.Close()

        inputReader := bufio.NewReader(inputFile)

        inputString, err := inputReader.ReadString('\n')

        if err != nil {
                fmt.Println("error!")
        }

        stringdata := strings.Split(inputString, "=")
        log.Println(stringdata[1])
        stringlist := strings.Split(stringdata[1], ",")
        return stringlist

}
