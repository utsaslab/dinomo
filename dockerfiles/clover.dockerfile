FROM sekwonlee/dinomo:base

MAINTAINER Sekwon Lee <sekwonlee90@gmail..com> version: 0.1

USER root

# Build DINOMO
WORKDIR $DINOMO_HOME
RUN git pull https://github.com/utsaslab/dinomo.git
RUN bash scripts/build.sh -j16 -bRelease -g -c
WORKDIR /

COPY start-clover.sh /
CMD bash start-clover.sh $SERVER_TYPE
