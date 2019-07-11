package main

import (
	"fmt"
	"time"

	beanstalk "github.com/beanstalkd/go-beanstalk"
)

func main() {
	fmt.Println("Starting import worker")

	conn, err := beanstalk.Dial("tcp", "localhost:11300")
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	tube := &beanstalk.Tube{Conn: conn, Name: "transform-queue"}
	for _ = range time.Tick(time.Duration(5) * time.Second) {
		data := []byte(fmt.Sprintf("{\"time\":\"%d\"}", time.Now().Unix()))
		_, err := tube.Put(data, 1, 0, time.Minute)
		if err != nil {
			panic(err)
		}
	}
}
