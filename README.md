# Hazeltest

Welcome, fellow Hazelcast warrior! Maybe you're taking a look at this repository because you've been facing the
challenge of load-testing your Hazelcast clusters, and perhaps you've been wondering whether there are tools out there
in the depths of this thing called the _Internet_ that can support you on this journey.

## Application Purpose

Hazeltest is a small application whose purpose is to support Hazelcast operation engineers in load-testing the Hazelcast
clusters described by their release candidates (where the _release candidate_ is the package bundling the Hazelcast
Platform executable plus all configuration to make it fly that might eventually make it to production, such as a Helm
chart). For this purpose, the application offers simple-to-configure, yet effective and versatile test loops you can
utilize to create load on the Hazelcast cluster under test, so errors such as misconfigurations make themselves known in
a safe testing environment -- that is, _long before the release candidate describing this cluster makes its way to
production_, where such errors could wreak all kinds of havoc!

Even if running an exhaustive and thorough load test on a Hazelcast cluster does not make misconfigurations manifest, it may still uncover potential for improvement in terms of the members' performance, which you can then address prior to shipping the release candidate in question to production. And in case neither errors nor improvement potentials make themselves known, then, well, you've got proof that your release candidate is ready to go!

In short, Hazeltest (currently) offers...

* ... two map runners along with two test loops to create load on Hazelcast maps
* ... two queue runners with a single test loop to create load on Hazelcast queues
* ... a chaos monkey to purposefully kill Hazelcast members in order to measure their configuration's appropriateness in
   terms of handling such error scenarios
* ... a status endpoint to query for test progress as a foundation for building automation on top of Hazeltest

Right now, the application is still in development, so it's likely this feature list will expand quite a bit in the
future!

Interested in a more elaborate overview of the background and ideas behind Hazeltest? Then the [introductory blog](https://nicokrieg.com/hazeltest-introduction.html) post I've written on precisely this matter has you covered.

## Getting Started

The following paragraphs will help you get started quickly with performing the first load test using Hazeltest, while
more in-depth information awaits you further down the line (to answer questions such as _What's a test loop as opposed to a runner, and how do I configure them?_, _What information does the status endpoint provide, and how could I build automation on top of it?_, and _What are some common flaws in Hazelcast configuration I should be cautious of?_).

If you have a Kubernetes cluster at your disposal, you're in luck, because the easiest and most convenient way to get started is to apply the various Helm charts you can find in this repository's [`charts`](./resources/charts/) folder to it (bonus luck points if the Kubernetes cluster in question has some juice in terms of CPU and memory, because that just makes everything so much more interesting!).

> :warning: **Note:** The various Helm charts you're going to install in scope of this section will spawn Pods that require a certain amount of resources (in terms of CPU and memory) on the target Kubernetes cluster. I configured the resource requests and limits such that all workloads are runnable on a single-node cluster with 6 CPUs and 20 GBs of RAM, assuming a lightweight Kubernetes flavor such as k3s. However, the workloads' resource requests and limits might not be optimally suited for your environment, so please feel free to adjust as needed.

### Installing Hazelcast

First, you can spawn a small Hazelcast cluster by invoking the following command (assuming, as will all ensuing commands, you're in the `resources/charts` folder of your locally cloned version of this repository):

```bash
helm upgrade --install hazelcastwithmancenter ./hazelcastwithmancenter --namespace=hazelcastplatform --create-namespace
```

If you left the Helm chart in question unmodified, this will bring up a three-node Hazelcast cluster plus a tiny Management Center instance. By default, the latter is exposed via a `NodePort`-type Service on port `30080`, so in case you want to check out the Management Center's UI, simply visit `http://<ip-of-an-arbitrary-node-in-your-kubernetes-cluster>:30080` in your browser. 

You can retrieve the IP of one of the nodes in your Kubernetes cluster with a command like the following:

```bash
k get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' && echo
```

... where `k` is an alias for `kubectl`, because typing the latter a million times a day when working with Kubernetes gets old real fast.



### Installing Hazeltest

Once the Hazelcast cluster is up and running -- and you have, optionally, brought up the Management Center's UI in your browser --, you can install Hazeltest like so:

```bash
helm upgrade --install hazeltest ./hazeltest --namespace=hazelcastplatform
```

 The Hazeltest Pod's logs will inform about the various actions the application's Map Runners and Queue Runners are performing, such as:

 * `starting operation chain of length 3000 for map '<some map>' on goroutine <z>`
 * `using upper boundary <x> and lower boundary <y> for map '<some map>' on goroutine <z>`
 * `finished <x> of <y> put runs for queue '<some queue>' in queue goroutine <z>`

 ... all neatly formatted in JSON for improved machine-based processing (log aggregation platforms like Splunk, for example, are excellent at working with JSON!) and along with quite a bit of meta information, but you get the idea.

 (In case you're wondering right now what the heck "Map Runners" and "Queue Runners" are -- don't worry! We'll dive into these concepts further down below.)

 


## Diving Deeper

The next step you may be inclined to do is to take a closer look at which test runners are currently available in the
application and how they can be configured.

### Available Runners

The first runner available today is the `PokedexRunner`, which runs the test loop with the 151 Pokémon of the
first-generation Pokédex. It serializes them into a string-based Json structure, which is then saved to Hazelcast.
The `PokedexRunner` is not intended to put a lot of data into Hazelcast (i. e., it is not intended to load-test a
Hazelcast cluster in terms of its memory), but instead stresses the CPU. The second available runner, on the other hand,
is the `LoadRunner`, and as its name indicates, it is designed to "load up" the Hazelcast cluster under test with lots
of data such as to test the behavior of the cluster once its maximum storage capacity has been reached. As opposed to
the `PokedexRunner`, which is -- by nature of the data it works with -- restricted to 151 elements in each map,
the `LoadRunner` can be configured arbitrarily regarding the number of elements it should put into each map, and the
elements' size is configurable, too.

### Configuration

The default configuration resides right with the source code, and you can find it [here](./client/defaultConfig.yaml).
It contains all properties currently available for configuring the two aforementioned runners along with comments
shortly describing what each property does and what it can be used for.

You can find all properties for configuring the Hazeltest application itself in
the [`values.yaml`](./resources/charts/hazeltest/values.yaml) file of
the [Hazeltest Helm chart](./resources/charts/hazeltest/) along with comments explaining them.

## Monitoring Your Hazelcast Cluster

Once you deployed Hazeltest so it generates some load on your Hazelcast cluster under test, you might want to start
monitoring it in order to get a more thorough understanding for the sort of load Hazeltest creates and how your
cluster's members can deal with that load. In case you have deployed Hazelcast to a Kubernetes cluster, you may find the
small monitoring stack this repository offers useful. In short, it utilizes the following components:

* Prometheus for scraping the Pods of your Hazelcast cluster (or clusters)
* Grafana dashboards for visualizing the metrics scraped by Prometheus
* Grafana itself

The latter two are provided by the [`padogrid-hazelmon`](https://hub.docker.com/r/antsinmyey3sjohnson/padogrid-hazelmon)
image, which itself is based on a [`padogrid`](https://hub.docker.com/r/padogrid/padogrid) image. (In case you're
wondering what PadoGrid is and how it can help you, there's a short introduction down below.)

### Installing Hazelcast

To make the monitoring stack work, both the Community and Enterprise edition of Hazelcast will do fine, as both offer
the _com_hazelcast_ metrics the Grafana dashboards rely on. In case you would still like to use the Enterprise version
using the chart provided in this repository, check out the [_Installing Hazelcast
Enterprise_](#installing-hazelcast-enterprise) section down below.

You can deploy the Hazelcast cluster to be monitored using the same old Helm command you may have already encountered in
the [_Getting Started_](#getting-started) section above (assuming you're in
the [`resources/charts`](./resources/charts/) directory of your local copy of this repository):

```bash
helm upgrade --install hazelcastwithmancenter ./hazelcastwithmancenter --namespace=hazelcastplatform --create-namespace
```

### Installing Prometheus

Once your Hazelcast cluster is up and running, it's time to scrape some metrics! To do so, you can install Prometheus
using the following command:

```bash
helm upgrade --install prometheus ./prometheus --namespace prometheus --create-namespace
```

(You can modify the Prometheus configuration in the chart's [`values.yaml`](./resources/charts/prometheus/values.yaml)
file if you so desire, of course, but the properties already provided there should work out of the box.)

You can check whether the _com_hazelcast_ metrics are scraped correctly by navigating to Prometheus' web UI. The URL for
doing so corresponds to the following pattern: `http://<external ip of prometheus loadbalancer service>:9090`

### Installing `padogrid-hazelmon`

The last puzzle piece in this small monitoring stack is
the [`padogrid-hazelmon`](https://hub.docker.com/r/antsinmyey3sjohnson/padogrid-hazelmon) image that comes bundled both
with Grafana and some really sweet dashboards for monitoring Hazelcast (more precisely speaking, the image's base
image, [`padogrid`](https://hub.docker.com/r/padogrid/padogrid), offers all the dashboards, and `padogrid-hazelmon`
merely adds Grafana and performs some work to get Grafana configured and running when deployed in a Helm chart).

The `padogrid-hazelmon` installation must be pointed to the Prometheus server that scrapes the _com_hazelcast_ metrics
from your Hazelcast cluster's members. In case you installed Prometheus according to the instructions above without
having modified the chart, the `padogridwithgrafana` chart will work out of the box, too. In case you have modified
either the name of the Kubernetes Service that points to your Prometheus Pod, the namespace, or the port, please make
sure to adjust the `PADO_MONITORING_PROMETHEUS_URL` property beneath the `padogridWithGrafana.config.padoEnv` object in
the chart's [`values.yaml`](./resources/charts/padogridwithgrafana/values.yaml) file accordingly.

You can install the chart like so:

```bash
helm upgrade --install padogridwithgrafana ./padogridwithgrafana --namespace hazelcastplatform
```

### Harvesting The Fruits

Finally, you'll probably want to get some monitoring done using this freshly installed monitoring stack. To do so, visit
the Grafana web UI and navigate to the _Dashboards_ page:

```
http://<external ip for grafana loadbalancer service>:3000/dashboards
```

> :warning: **Note:** If this is your first login to Grafana, you can log in using Grafana's default username/password
> combination, which, at the time of this writing, is _admin/admin_. Of course, it is recommended to change this to
> something more secure once Grafana prompts you to do so.

Here, the available dashboards are categorized into multiple folders:

![Overview of dashboards available in Grafana](./resources/images_for_readme/grafana_dashboards_overview.png)

A good place to start is the _00Main_ dashboard in the _Hazelcast_ folder. Wih a very small Hazelcast cluster, it might
look something like the following:

![Dashboard showing an overview of the state of a small Hazelcast cluster.](./resources/images_for_readme/grafana_dashboard_hazelcast_00main.png)

This should give you a good overview of how your Hazelcast cluster is currently doing, and more detailed views are
available via the links on the left-hand side of the dashboard. In fact, there is quite a lot to discover, so feel free
to dig in and have fun exploring!

## Installing Hazelcast Enterprise

In case you would like to install Hazelcast in the Enterprise edition using the chart offered in this repository, you
may find the following notes useful.

The [Helm chart](./resources/charts/hazelcastwithmancenter/) included in this repository for installing Hazelcast can be
configured to use Hazelcast Enterprise rather than the community edition. The following properties in
the [`values.yaml`](./resources/charts/hazelcastwithmancenter/values.yaml) file are important in this context:

* `.Values.platform.cluster.members.edition.enterprise.enabled`: Whether to enable using the Enterprise edition. If set
  so `true`, the chart expects a Kubernetes Secret that contains the enterprise license key, see below.
* `.Values.platform.cluster.members.edition.enterprise.image`: The Hazelcast Enterprise image to use.
* `.Values.platform.cluster.members.edition.enterprise.license.secretName`: The name of the Kubernetes Secret that
  contains the enterprise license key.
* `.Values.platform.cluster.members.edition.enterprise.license.keyPath`: The path, within the secret, to the key that
  holds the enterprise license key as a string.

> :warning: **Note:** The Hazelcast Enterprise cluster must be deployed to the same namespace that holds the Secret
> containing the license key.

For example, assuming you would like to use the image for Hazelcast Enterprise 5.3.6 and your license key sits in a
Secret called `hazelcast-enterprise-license` that represents the license key string a in a property
called `data.licenseKey`, you would configure the properties above like so:

```yaml
platform:
  # ...
  cluster:
    # ...
    members:
      # ...
      edition:
        enterprise:
          enable: true
          image: hazelcast/hazelcast-enterprise:5.3.6
          license:
            secretName: hazelcast-enterprise-license
            keyPath: licenseKey
```

Then, you can install your Hazelcast cluster using the very same Helm command that, by now, is probably familiar:

```bash
helm upgrade --install hazelcastwithmancenter ./hazelcastwithmancenter --namespace=hazelcastplatform --create-namespace
```

## Generating Load With PadoGrid

_PadoGrid_ is an open source application that provides a fantastic playing ground for testing all kinds of data grid and
computing technologies (Hazelcast is one of them, but since it's based on what the developer calls _distributed
workspaces_ and pluggable _bundles_, it also works with other technologies like Spark, Kafka, and Hadoop).

There are different sub-programs available in PadoGrid, one of which is the [
_perf_test_ application for Hazelcast](https://github.com/padogrid/padogrid/wiki/Hazelcast-perf_test-App). This handy
tool offers the capability of running tests that can be configured by means of text-based properties files that describe
the groups and operations to run in scope of a test. If your goal is to load-test your Hazelcast cluster in terms of
memory and CPU only (rather than CPU and memory plus the number of maps and clients), then PadoGrid will perfectly suit
your needs.

On top of that, the most recent versions of PadoGrid (starting with v0.9.30) also contain super useful dashboards for
monitoring Hazelcast clusters, and the `padogrid-hazelmon` image you may have encountered if you set up Hazeltest's
monitoring stack according to the instructions above leverages them in a running Grafana instance you can access and use
without much prior configuration work.

You can find PadoGrid's source code and many useful guides for getting started over
on [GitHub](https://github.com/padogrid/padogrid).

## More text to potentially include later on

Designed to run on Kubernetes (but by this design by no means limited to running there), the application embodies an
important Kubernetes design philosophy: to formulate desired target state declaratively and in a format easy to
understand and reason about by engineers (granted, it may be subject to wonderful discussion whether Yaml is actually
easy to read assuming a sufficiently large file -- or whether Yaml is something humanity was in need of in the first
place --, but that's beside the point). While _desired target state_ is hard to capture in terms of application
configuration if what the application does is create load on a Hazelcast cluster, Hazeltest still lets you define
declaratively what you want its actors to do -- i.e., what load you want them to create -- by means of
easy-to-reason-about Yaml files.

This reveals another purpose of Hazeltest besides offering the means to perform effective, automated Hazelcast
load-testing: to ensure _repeatability_ of those tests. Repeatability is crucial, for example, when you're adjusting
Hazelcast configuration and want to measure the impact on performance of your adjustments, or when you wish to assess
the performance of a new Hazelcast version compared to an older one. Need to examine how performance changes when you
swap out the Hazelcast JVM's Garbage Collector (say, G1GC vs. ZGC)? Want to know how halving the batch size of a WAN
replication publisher affects replication latency? Or how about simply checking whether your memory configuration is
sane? Only automated load tests with good repeatability will help you get meaningful results quickly and reliably! With
Hazeltest, you can check in Yaml load configs to version control and 