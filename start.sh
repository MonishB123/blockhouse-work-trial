#!/bin/bash

trap "sudo docker-compose down -v" EXIT

sudo docker-compose up --build