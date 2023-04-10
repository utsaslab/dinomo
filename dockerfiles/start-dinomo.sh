#!/bin/bash

if [ -z "$1" ]; then
  echo "No argument provided. Exiting."
  exit 1
fi

# A helper function that takes a space separated list and generates a string
# that parses as a YAML list.
gen_yml_list() {
  IFS=' ' read -r -a ARR <<< $1
  RESULT=""

  for IP in "${ARR[@]}"; do
    RESULT=$"$RESULT        - $IP\n"
  done

  echo -e "$RESULT"
}

cd $DINOMO_HOME

git pull https://github.com/utsaslab/dinomo.git

mkdir -p conf

#PRIVATE_IP=`ifconfig eno1 | grep 'inet' | grep -v inet6 | sed -e 's/^[ \t]*//' | cut -d' ' -f2`
PRIVATE_IP=`hostname -I | awk '{print $1}'`
PUBLIC_IP=$PRIVATE_IP

# Compile the latest version of the code on the branch we just check out.
cd build && make -j8 && cd ..

# Do not start the server until conf/dinomo-config.yml has been copied onto this
# pod -- if we start earlier, we won't now how to configure the system.
while [[ ! -f "conf/dinomo-config.yml" ]]; do
  continue
done

# Tailor the config file to have process specific information.
if [ "$1" = "mn" ]; then
  echo -e "monitoring:" >> conf/dinomo-config.yml
  echo -e "    mgmt_ip: $MGMT_IP" >> conf/dinomo-config.yml
  echo -e "    ip: $PRIVATE_IP" >> conf/dinomo-config.yml

  ./build/target/kvs/dinomo-monitor
elif [ "$1" = "r" ]; then
  echo -e "routing:" >> conf/dinomo-config.yml
  echo -e "    ip: $PRIVATE_IP" >> conf/dinomo-config.yml

  LST=$(gen_yml_list "$MON_IPS")
  echo -e "    monitoring:" >> conf/dinomo-config.yml
  echo -e "$LST" >> conf/dinomo-config.yml

  ./build/target/kvs/dinomo-route
elif [ "$1" = "b" ]; then
  echo -e "user:" >> conf/dinomo-config.yml
  echo -e "    ip: $PRIVATE_IP" >> conf/dinomo-config.yml

  LST=$(gen_yml_list "$MON_IPS")
  echo -e "    monitoring:" >> conf/dinomo-config.yml
  echo -e "$LST" >> conf/dinomo-config.yml

  LST=$(gen_yml_list "$ROUTING_IPS")
  echo -e "    routing:" >> conf/dinomo-config.yml
  echo -e "$LST" >> conf/dinomo-config.yml

  ./build/target/benchmark/dinomo-bench
elif [ "$1" = "bt" ]; then
  echo -e "trigger:" >> conf/dinomo-config.yml

  LST=$(gen_yml_list "$BENCH_IPS")
  echo -e "    benchmark:" >> conf/dinomo-config.yml
  echo -e "$LST" >> conf/dinomo-config.yml

  LST=$(gen_yml_list "$MGMT_IPS")
  echo -e "    management:" >> conf/dinomo-config.yml
  echo -e "$LST" >> conf/dinomo-config.yml

  ./build/target/benchmark/dinomo-bench-trigger 64
else
  echo -e "server:" >> conf/dinomo-config.yml
  echo -e "    seed_ip: $SEED_IP" >> conf/dinomo-config.yml
  echo -e "    public_ip: $PUBLIC_IP" >> conf/dinomo-config.yml
  echo -e "    private_ip: $PRIVATE_IP" >> conf/dinomo-config.yml
  echo -e "    mgmt_ip: $MGMT_IP" >> conf/dinomo-config.yml

  LST=$(gen_yml_list "$MON_IPS")
  echo -e "    monitoring:" >> conf/dinomo-config.yml
  echo -e "$LST" >> conf/dinomo-config.yml

  LST=$(gen_yml_list "$ROUTING_IPS")
  echo -e "    routing:" >> conf/dinomo-config.yml
  echo -e "$LST" >> conf/dinomo-config.yml

  ./build/target/kvs/dinomo-kvs
fi
