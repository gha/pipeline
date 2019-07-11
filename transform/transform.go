package main

import (
	"encoding/json"
	"fmt"
	"time"

	beanstalk "github.com/beanstalkd/go-beanstalk"
)

func main() {
	fmt.Println("Starting transform worker")

	conn, err := beanstalk.Dial("tcp", "localhost:11300")
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	tubeSet := beanstalk.NewTubeSet(conn, "transform-queue")
	tubeTo := &beanstalk.Tube{Conn: conn, Name: "load-queue"}
	for {
		id, body, err := tubeSet.Reserve(time.Minute)
		if err != nil {
			panic(err)
		}

		data := make(map[string]string)
		if err := json.Unmarshal(body, &data); err != nil {
			panic(err)
		}

		data["foo"] = "bar"

		event, err := json.Marshal(data)
		if err != nil {
			panic(err)
		}

		if _, err := tubeTo.Put(event, 1, 0, time.Minute); err != nil {
			panic(err)
		}

		if err := conn.Delete(id); err != nil {
			panic(err)
		}
	}
}
