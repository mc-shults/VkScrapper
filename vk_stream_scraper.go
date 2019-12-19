package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"
)

type Config struct {
	Host     string
	Key      string
	MongoURL string
}

func check(e error) {
	if e != nil {
		log.Fatal("dial error:", e)
	}
}

func initConfig(filename string) Config {
	dat, err := ioutil.ReadFile(filename)
	check(err)

	var conf Config
	json.Unmarshal([]byte(dat), &conf)
	return conf
}

func connectVk(streamURL url.URL) (connection *websocket.Conn) {
	fmt.Printf("connecting to %s\n", streamURL.String())
	connection, response, err := websocket.DefaultDialer.Dial(streamURL.String(), nil)
	if err != nil {
		connection = nil
		if err == websocket.ErrBadHandshake {
			log.Printf("handshake failed with status %d\n", response.StatusCode)
			bodyBuf, _ := ioutil.ReadAll(response.Body)
			log.Fatal("respBody:", string(bodyBuf))
		}
		log.Fatal("dial error:", err)
	} else {
		fmt.Println("connection established")
	}
	return
}

func work(connection *websocket.Conn, done *chan struct{}, dbPosts *mongo.Collection) {
	for {
		_, message, err := connection.ReadMessage()
		if err != nil {
			log.Println("read error:", err)
			*done <- struct{}{}
			return
		}
		var responseObj map[string]interface{}
		json.Unmarshal([]byte(message), &responseObj)
		if responseObj["code"].(float64) != 100 {
			log.Printf("recv error: %s", string(message))
			continue
		}
		insertResult, err := dbPosts.InsertOne(context.TODO(), responseObj["event"])
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Inserted a single document: ", insertResult.InsertedID)
	}
}

func waitEnd(connection *websocket.Conn, done *chan struct{}) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	select {
	case <-interrupt:
		fmt.Println("interrupt")
		err := connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Println("write close error: ", err)
			return
		}
		select {
		case <-*done:
		case <-time.After(time.Second):
		}
	case <-*done:
	}
}

func connectMongo(mongoURL string) *mongo.Client {
	db, err := mongo.NewClient(options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")
	return db
}

func main() {
	conf := initConfig("config.json")
	streamURL := url.URL{Scheme: "wss", Host: conf.Host, Path: "/stream/", RawQuery: "key=" + conf.Key}
	connection := connectVk(streamURL)
	defer connection.Close()

	db := connectMongo(conf.MongoURL)
	dbPosts := db.Database("big_data").Collection("posts")
	defer db.Disconnect(context.TODO())

	done := make(chan struct{})
	defer close(done)
	go work(connection, &done, dbPosts)
	waitEnd(connection, &done)
}
