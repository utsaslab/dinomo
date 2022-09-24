#!/bin/bash

sudo docker image rm $(sudo docker image ls --format '{{.Repository}} {{.ID}}' | grep 'sekwonlee' | awk '{print $2}')

cd cluster/dockerfiles
sudo docker build . -f dinomo-base.dockerfile -t sekwonlee/dinomo:base
sudo docker push sekwonlee/dinomo:base

cd cluster
sudo docker build . -f management.dockerfile -t sekwonlee/dinomo:management
sudo docker push sekwonlee/dinomo:management

cd ../../../dockerfiles
sudo docker build . -f dinomo.dockerfile -t sekwonlee/dinomo:kvs
sudo docker push sekwonlee/dinomo:kvs

sudo docker build . -f clover.dockerfile -t sekwonlee/dinomo:clover
sudo docker push sekwonlee/dinomo:clover

sudo docker build . -f asymnvm.dockerfile -t sekwonlee/dinomo:asymnvm
sudo docker push sekwonlee/dinomo:asymnvm

cd ../
