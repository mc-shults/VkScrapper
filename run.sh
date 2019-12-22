#!/bin/bash

sudo docker image build -t scrapper:0.1.$1 .
sudo docker run --detach --name scrapper scrapper:0.1.$1
