#!/usr/bin/env bash

minikube start --driver docker --container-runtime docker --gpus all
kubectl create ns moriarty
