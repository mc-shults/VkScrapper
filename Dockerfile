FROM golang

WORKDIR /go/src/vk_stream_scraper
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...

CMD ["mod", "-host", "streaming.vk.com", "-key", "4431e6ab5e6a187317b676b1cc20db3b2283957e", "-mongo-url", "mongodb://big_data_user:BiGDatA@34.222.199.17"]
