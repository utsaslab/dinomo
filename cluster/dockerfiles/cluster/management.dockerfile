FROM sekwonlee/dinomo:base

MAINTAINER Sekwon Lee <sekwonlee90@gmail.com> version: 0.1

USER root

# Install dependencies to use Kubespray
WORKDIR /DINOMO/kubespray
RUN pip3 install -r requirements.txt
RUN pip3 install numpy
WORKDIR /

# Install kubectl. Downloads a precompiled executable and copies it into place.
RUN wget -O kubectl https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin/kubectl

# Create the directory where the kubecfg is stored. The startup scripts will
# copy the actual kubecfg for the cluster into this directory at startup time.
RUN mkdir $HOME/.kube
RUN mkdir $HOME/.ssh

COPY start-management.sh /
CMD bash start-management.sh
