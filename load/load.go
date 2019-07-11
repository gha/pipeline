package main

import (
	"encoding/json"
	"fmt"
	"time"

	beanstalk "github.com/beanstalkd/go-beanstalk"
)

func main() {
	fmt.Println("Starting load worker")

	conn, err := beanstalk.Dial("tcp", "localhost:11300")
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	tubeSet := beanstalk.NewTubeSet(conn, "load-queue")
	for {
		id, body, err := tubeSet.Reserve(time.Minute)
		if err != nil {
			panic(err)
		}

		data := make(map[string]string)
		if err := json.Unmarshal(body, &data); err != nil {
			panic(err)
		}

		fmt.Printf("Loaded job %d\n", id)

		if err := conn.Delete(id); err != nil {
			panic(err)
		}
	}
}
