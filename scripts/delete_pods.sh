#!/bin/bash

sudo kubectl delete daemonset --all
sudo kubectl delete pod --all

sudo kubectl label node $(sudo kubectl get nodes | grep worker | awk '{print $1}') role-
sudo kubectl label node $(sudo kubectl get nodes | grep router | awk '{print $1}') role-
sudo kubectl label node $(sudo kubectl get nodes | grep bench | awk '{print $1}') role-
