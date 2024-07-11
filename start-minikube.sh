#!/usr/bin/env bash

minikube start --driver docker --container-runtime docker --gpus all
minikube addons enable metrics-server
kubectl create ns moriarty
