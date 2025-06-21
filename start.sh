#!/bin/bash

trap "docker-compose down -v" EXIT

docker-compose up --build