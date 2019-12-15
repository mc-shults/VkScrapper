package main

import (
	"context"
	"encoding/json"
	"flag"
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

type consoleParams struct {
	host     string
	key      string
	mongoURL string
	help     bool
}

var (
	argv consoleParams
)

func processArgs() (needStop bool) {
	needStop = true

	if argv.help {
		flag.Usage()
	} else if len(argv.host) == 0 {
		log.Println("-host is required")
		flag.Usage()
	} else if len(argv.key) == 0 {
		log.Println("-key is required")
		flag.Usage()
	} else if len(argv.mongoURL) == 0 {
		log.Println("-mongo-url is required")
		flag.Usage()
	} else {
		needStop = false
	}

	return
}

func init() {
	flag.StringVar(&argv.host, `host`, ``, `streaming api host. REQUIRED`)
	flag.StringVar(&argv.key, `key`, ``, `client key. REQUIRED`)
	flag.StringVar(&argv.mongoURL, `mongo-url`, ``, `url of mongodb. REQUIRED`)
	flag.BoolVar(&argv.help, `h`, false, `show this help`)

	flag.Parse()
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
		fmt.Printf("recv: %s", string(message))
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
	if processArgs() {
		return
	}
	streamURL := url.URL{Scheme: "wss", Host: argv.host, Path: "/stream/", RawQuery: "key=" + argv.key}
	connection := connectVk(streamURL)
	defer connection.Close()

	db := connectMongo(argv.mongoURL)
	dbPosts := db.Database("bigdata").Collection("posts")
	defer db.Disconnect(context.TODO())

	done := make(chan struct{})
	defer close(done)
	go work(connection, &done, dbPosts)
	waitEnd(connection, &done)
}
