# Hydro Cluster Management

[![Build Status](https://travis-ci.com/hydro-project/cluster.svg?branch=master)](https://travis-ci.com/hydro-project/cluster)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


This repository contains the cluster creation and management components of the Hydro stack. 

The `hydro.cluster` package contains scripts to create and scale Hydro clusters using [Kubernetes](https://kubernetes.io) and [kops](http://github.com/kubernetes/kops/). You can find a detailed getting started guide on spinning up a new cluster on using AWS [here](docs/getting-started-aws.md). We have not tested running Hydro on other cloud providers, but `kops` does support other providers. If you are interested in extending Hydro to run in other environments, please [open an issue](https://github.com/hydro-project/cluster/issues/new) or [email us](mailto:vikrams@cs.berkeley.edu,cgwu@berkeley.edu). 

The `hydro.management` package has a management server that runs _inside_ a Hydro cluster, performs some basic metadata management for the [Anna KVS](https://github.com/hydro-project/anna), and runs the policy engine for the [Cloudburst serverless platform](https://github.com/hydro-project/cloudburst).

## License

The Hydro Project is licensed under the [Apache v2 License](LICENSE).
