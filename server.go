package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/hectorandac/kafka/libraries"
	"github.com/hectorandac/kafka/libraries/kafka"
)

var reader = bufio.NewReader(os.Stdin)

func Processor(message string) string {
	text := libraries.Text{Body: message}
	text.Process()
	return fmt.Sprintf("%s", text)
}

func main() {
	choice := captureUserInput()

	switch choice {
	case "c":
		kkc := kafka.KafkaClient{Context: kafka.Consumer, Server: "localhost", Topic: "main", Group: "common_base"}
		kkc.Init()
		kkc.CreateTopic(10, 1)
		kkc.Consum(Processor)
	case "p":
		kkc := kafka.KafkaClient{Context: kafka.Producer, Server: "localhost", Topic: "main", Group: "common_base"}
		kkc.Init()
		kkc.CreateTopic(10, 1)
		for {
			kkc.Produce(captureUserInput())
		}
	case "i":
		kkc := kafka.KafkaClient{Context: kafka.Consumer, Server: "localhost", Topic: "main", Group: "common_base"}
		kkc.Init()
		kkc.CreateTopic(10, 1)
		kkc.PrintClusterInfo()
	}

}

func captureUserInput() string {
	fmt.Print("-> ")
	text, _ := reader.ReadString('\n')
	return strings.Replace(text, "\n", "", -1)
}
