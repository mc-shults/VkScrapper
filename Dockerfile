FROM golang

WORKDIR /go/src/vk_stream_scraper
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...

CMD ["mod"]
