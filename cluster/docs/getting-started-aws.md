# Getting Started on AWS

To run Hydro in cluster mode (i.e., on multiple nodes and with autoscaling enabled), you will need to have an AWS account. Hydro depends on [Kubernetes](http://kubernetes.io). This tutorial will walk you through setting up the required dependencies to run the Kubernetes CLI (`kubectl`) and [kops](https://github.com/kubernetes/kops) (a tool to create & manage Kubernetes clusters on public cloud infrastructure).

### Prerequisites

We assume you are running inside an EC2 linux VM on AWS, where you have Python3 installed (preferably Python3.6 or later -- we have not tested with earlier versions).
AWS has default quotas on resources that can be allocated for accounts. The cluster to create in this doc will exceed the default vCPU limit(32) for a regular AWS account. Please make sure this limit is lifted before proceeding. 
[read more](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-resource-limits.html). 

### Step 0: Installing the Hydro project repositories on your VM

* Run the following commands to clone the various git repositories for
  the Hydro project and configure the `HYDRO_HOME` environment variable:

```bash
# you can change ~/hydro-project to whatever you like
export HYDRO_HOME=~/hydro-project

mkdir $HYDRO_HOME
cd $HYDRO_HOME
git clone --recurse-submodules https://github.com/hydro-project/anna.git
git clone --recurse-submodules https://github.com/hydro-project/anna-cache.git
git clone --recurse-submodules https://github.com/hydro-project/cluster.git
git clone --recurse-submodules https://github.com/hydro-project/cloudburst.git

cd cluster
```

### Step 1: Installing `kubectl`, `kops`, & friends on your VM

* Install the `kubectl` binary using the Kubernetes documentation, found [here](https://kubernetes.io/docs/tasks/tools/install-kubectl). Don't worry about setting up your kubectl configuration yet.
* Install the `kops` binary -- documentation found [here](https://github.com/kubernetes/kops/blob/master/docs/install.md)
* Install a variety of Python dependencies: `pip3 install awscli boto3 kubernetes`<sup>1</sup>
* Download and install the AWS CLI [here](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).

### Step 2: Configuring `kops`

* `kops` requires an IAM group and user with permissions to access EC2, Route53, etc. You can find the commands to create these permissions [here](https://github.com/kubernetes/kops/blob/master/docs/getting_started/aws.md#aws). Make sure that you capture the Access Key ID and Secret Access Key for the `kops` IAM user and set them as environmnent variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) and pass them into `aws configure`, as described in the above link.
* `kops` also requires an S3 bucket for state storage. More information about configuring this bucket can be found [here](https://github.com/kubernetes/kops/blob/master/docs/getting_started/aws.md#cluster-state-storage).
* Create a service linked role for the Elastic Load Balancer (ELB) service: `aws iam create-service-linked-role --aws-service-name "elasticloadbalancing.amazonaws.com"`. Hydro uses ELBs as the entrypoint to the system for both Anna and Cloudburst.
* Finally, in order to access the cluster, you will need a domain name<sup>2</sup> to point to. Currently, we have only tested our setup scripts with domain names registered in Route53. `kops` supports a variety of DNS settings, which you can find more information about [here](https://github.com/kubernetes/kops/blob/master/docs/getting_started/aws.md#configure-dns). If you would like help with running using other DNS settings and run into any challenges, please please [open an issue](https://github.com/hydro-project/cluster/issues/new), and we'd be happy to help debug.

### Step 3: Odds and ends

* Our cluster creation scripts depend on three environment variables: `HYDRO_HOME`, `HYDRO_CLUSTER_NAME`, and `KOPS_STATE_STORE`. Set `HYDRO_HOME` as described in Step 0. Set the `HYDRO_CLUSTER_NAME` variable to the name of the Route53 domain that you're using (see Footnote 2 if you are not using a Route53 domain -- you will need to modify the cluster creation scripts). Set the `KOPS_STATE_STORE` variable to the S3 URL of S3 bucket you created in Step 2 (e.g., `s3://hydro-kops-state-store`).
* As described in Footnote 1, make sure that your `$PATH` variable includes the path to the `aws` CLI tool. You can check if its on your path by running `which aws` -- if you see a valid path, then you're set.
* As descried in Step 2, make sure you have run `aws configure` and set your region (by default, we use `us-east-1`) and the access key parameters for the kops user created in Step 2.

### Step 4: Creating your first cluster

You're now ready to create your first cluster. To start off, we'll create a tiny cluster, with one memory tier node and one routing node. From the `$HYDRO_HOME/cluster/` directory, run `python3 -m hydro.cluster.create_cluster -m 1 -r 1 -f 1 -s 1`. This will take about 10-15 minutes to run. Once it's finished, you will see the URL of two AWS [ELB](https://aws.amazon.com/elasticloadbalancing/)s, which can be used to interact with the Anna KVS and Cloudburst, respectively.

<sup>1</sup> By default, the AWS CLI tool installs in `~/.local/bin` on Ubuntu. You will have to add this directory to your `$PATH`.

<sup>2</sup> You can also run in local mode, where you set the `HYDRO_CLUSTER_NAME` environment variable to `{clustername}.k8s.local`. This setting doesn't require a domain name -- however, this mode limits cluster size because it only runs in mesh networking mode (which only allows up to 64 nodes, from what we can tell), and requires modifying our existing cluster creation scripts. We don't have documentation written up for this as its not a use case we intend to support, but you can either [open an issue](https://github.com/hydro-project/cluster/issues/new) or send us an [email](mailto:vikrams@cs.berkeley.edu,cgwu@berkeley.edu) if you're interested in this.
