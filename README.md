# Hazeltest

Welcome, fellow Hazelcast warrior! Maybe you're taking a look at this repository because you've been facing the
challenge of load-testing your Hazelcast clusters, and perhaps you've been wondering whether there are tools out there
in the depths of this thing called the _Internet_ that can support you on this journey.

## Application Purpose

Short disclaimer: If you're more the visual type of person and would much rather digest a video than read text, then the [introduction video for Hazeltest](https://youtu.be/GojMTeDzdsk?si=xh-6P6Vk15UdjWCb) might be for you!

On the other hand, in case you like _reading_ stuff -- you know, how our grandfathers did --, then... well, read on.

Hazeltest is a small application whose purpose is to support Hazelcast operation engineers in load-testing the Hazelcast clusters described by their release candidates (where the _release candidate_ is the package bundling the Hazelcast Platform executable plus all configuration to make it fly that might eventually make it to production, such as a Helm chart). Along those lines, the application offers simple-to-configure, yet effective and versatile runners you can utilize to create load on the Hazelcast cluster under test, so errors such as misconfigurations make themselves known in a safe testing environment -- that is, _long before the release candidate describing this cluster makes its way to production_, where such errors could wreak all kinds of havoc!

Keep in mind that you don't really have a choice _if_ your Hazelcast clusters will be load-tested; you can only choose _when_ -- after all, in the absence of any kind of formal load-testing prior to shipping a release candidate to production, it's the production environment itself that will necessarily conduct the first load test, as it will be the first stage in which the release candidate -- now actually released -- gets exposed to load, which it must then handle. So, if load testing is necessarily performed anyway, then why not adjust the _when_ bit to a point in the release cycle in which the release candidate can be load-tested to your heart's content in a safe environment, such as a dedicated load-testing environment? 

Even if running an exhaustive and thorough load test on a Hazelcast cluster does not make misconfigurations manifest, it may still uncover potential for improvement in terms of the members' performance, which you can then address prior to shipping the release candidate in question to production. And in case neither errors nor improvement potentials make themselves known, then, well, you've got proof that your release candidate is ready to go, and you brought about that proof long before the release candidate actually got shipped to production! (In my humble opinion, bringing that proof _before_ rolling out the thing to production just makes for so much better sleep at night, specifically when your Hazelcast clusters run mission-critical workloads.)

In short, Hazeltest (currently) offers...

* ... two map runners along with two test loops to create load on Hazelcast maps
* ... two queue runners with a single test loop to create load on Hazelcast queues
* ... a chaos monkey to purposefully kill Hazelcast members in order to measure their configuration's appropriateness in terms of handling such error scenarios
* ... a status endpoint to query for test progress as a foundation for building automation on top of Hazeltest

Hazeltest is under active development, so it's likely this feature list will expand quite a bit in the
future!

Interested in a more elaborate overview of the background and ideas behind Hazeltest? Then you might find the [introductory blog post](https://nicokrieg.com/hazeltest-introduction.html) I've written on precisely this matter helpful.

I've also been working on some videos explaining the idea of and concepts embedded in Hazeltest, which you can find on the [Hazeltest channel over on YouTube](https://www.youtube.com/@hazeltest) (in case you watched the introduction video linked to above, you may have already discovered the other videos, too).

## Getting Started

The following paragraphs will help you get started quickly with performing the first load test using Hazeltest, while more in-depth information awaits you further down the line (to answer questions such as _What's a test loop as opposed to a runner, and how do I configure them?_, _What information does the status endpoint provide, and how could I build automation on top of it?_, and _What are some common flaws in a Hazelcast configuration I should be cautious of?_).

### Simple Load Testing And Results Investigation

If you have a Kubernetes cluster at your disposal, you're in luck, because the easiest and most convenient way to get started is to apply the various Helm charts you can find in this repository's [`charts`](./resources/charts) folder to it (bonus luck points if the Kubernetes cluster in question has some juice in terms of CPU and memory, because that just makes everything so much more interesting!).

> :warning: **Note:** The various Helm charts you're going to install in scope of this section will spawn Pods that require a certain amount of resources (in terms of CPU and memory) on the target Kubernetes cluster (obviously -- d'uh). I configured the resource requests and limits such that all workloads are runnable on a single-node cluster with 6 CPUs and 20 GBs of RAM, assuming a lightweight Kubernetes flavor such as k3s. However, the workloads' resource requests and limits might not be optimally suited for your environment, so please feel free to adjust as needed.

#### Installing Hazelcast

First, you can spawn a small Hazelcast cluster by invoking the following command (assuming, as will all ensuing commands, you're in the `resources/charts` folder of your locally cloned version of this repository):

```bash
helm upgrade --install hazelcastwithmancenter ./hazelcastwithmancenter --namespace=hazelcastplatform --create-namespace
```

If you left the Helm chart in question unmodified, this will bring up a three-node Hazelcast cluster plus a tiny Management Center instance. By default, the latter is exposed via a `NodePort`-type Service on port `30080`, so in case you want to check out the Management Center's UI, simply visit `http://<ip-of-an-arbitrary-node-in-your-kubernetes-cluster>:30080` in your browser. 

You can retrieve the IP of one of the nodes in your Kubernetes cluster with a command like the following:

```bash
k get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}'; echo
```

... where `k` is an alias for `kubectl`, because typing the latter a million times a day when working with Kubernetes gets old real fast.

#### Installing Hazeltest

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

 > :warning: **Note:** By default, this chart launches a single Hazeltest instance whose Chaos Monkey feature is enabled, so worry not if you suddenly observe Hazelcast members getting terminated and restarting -- in this case, that's actually intended!

 #### Installing Prometheus

Isn't running any kind of load test so much more fun when you can watch some dashboard panels go wild as soon as the test starts doing its thing? Well, the Grafana chart you can find in this repository's charts folder comes with some nice dashboards, but they are of no use whatsoever without the delivery of corresponding metrics. 

The Hazelcast members you brought up earlier are configured to expose metrics to Prometheus (or any kind of technology able to scrape them, really), so let's get ourselves a Prometheus instance to perform the scraping:

```bash
helm upgrade --install prometheus ./prometheus -n monitoring --create-namespace
```

Note that this Prometheus instance is super simple -- it writes any metrics gathered to the Pod filesystem, for example, so as soon as the Pod's gone, so are the metrics. (Which is exactly what we want in a simple demo setup so as to save ourselves the hassle of setting up proper persistence and running clean-up jobs afterwards, but don't use this for production, obviously.)

By default, the Prometheus chart, too, exposes its workload by means of a `NodePort`-type Service, albeit this time on port `30090`. If you feel so inclined, therefore, you can once again fire up your browser and directly access Prometheus' UI, but without some queries to run, there won't be much to see. Luckily, you won't have to type out and then manually run said queries -- the dashboards bundled with the Grafana Helm chart you're about to install will do that for you.

#### Installing Grafana

We've established above that load testing is a lot more fun if you can watch the effects of the tests unfold on a couple of nice dashboards, but there are no such dashboards in sight yet. Let's change that!

The following command will bring up a small Grafana instance to visualize the metrics exposed by Hazelcast and scraped by Prometheus:

```bash
helm upgrade --install grafana ./grafana -n monitoring
```

The Grafana chart you just installed is a slightly altered version of the [official Grafana chart](https://github.com/grafana/helm-charts/tree/main/charts/grafana) that comes bundled with a couple of dashboards and pre-configured so the Grafana Pod doesn't use any kind of persistence (as that would increase the complexity of the setup, rendering it less viable for a simple "Getting Started" guide such as this one) and is easily accessible from outside. By default, Grafana's UI is accessible by means of a `NodePort`-type Service on port `30300`. Bring up said UI by pointing your browser to `http://<ip-of-an-arbitrary-node-in-your-kubernetes-cluster>:30300`.

Once the UI asks you to authenticate, use `admin` as the username and the password retrieved by the following command:

```bash
k get secret --namespace monitoring grafana -o jsonpath="{.data.admin-password}" | base64 --decode; echo
```

#### Harvesting The Fruits
After having successfully launched a small Hazelcast/Hazeltest stack plus a super simple monitoring stack on top, let's harvest the fruits of our work by investigating the aforementioned dashboards!

Unsurprisingly, they await you in the dashboard section's _Hazelcast_ folder:

![Dashboards available in the Hazelcast folder](./resources/images_for_readme/grafana_dashboards_in_hazelcast_folder.png)

By now, your Hazelcast/Hazeltest stack should have been running for at least a couple of minutes, so all dashboards should display quite a bit of interesting information. For example, the `System Overview` dashboard might look something like the following:

![General Hazelcast cluster statistics](./resources/images_for_readme/grafana_system_overview_general_cluster_statistics.png)

![Statistics specific to Hazelcast members](./resources/images_for_readme/grafana_system_overview_member_specific_statistics.png)

Here, the little spikes you can see are hints of terminated and then newly joined Hazelcast members, which are the results of the workings of Hazeltest's aforementioned Chaos Monkey.

At the time of this writing, Hazeltest comes with actors to create load on Hazelcast maps and queues (in the form of the aforementioned Map and Queue Runners, respectively), so it makes sense there are dashboards for these two kinds of data structures. The _Maps_ dashboard might look like this on your end if your Hazelcast/Hazeltest stack has been running for some time:

![Maps overview](./resources/images_for_readme/grafana_maps_overview.png)

Similarly, the _Queues_ dashboard may appear like so:

![Queues overview](./resources/images_for_readme/grafana_queues_overview.png)

These dashboards give rough indications for how Hazeltest's Map Runners and Queue Runners work (how they work as a result of the configuration encapsulated in the previously installed Hazeltest Helm chart, anyway), so in the next section, we're going to take a closer look at those runners and a concept called "Test Loops", but a little bit of ground-laying work is required first.

### Running Hazeltest Outside of Kubernetes
The Hazeltest Helm chart available in this repository is useful in the sense that it conveniently hides all the details of configuration necessary to connect Hazeltest to a target Hazelcast cluster at startup, but if you've come to this section, it's likely you'll want to know how to connect to a Hazelcast cluster from outside of Kubernetes (or without any involvement of Kubernetes whatsoever, really).

Well, you're in luck! You can either read on here, or in case you're more the audio/visual type, you may find the following video on my channel enlightening:

[How To Hazeltest 1: Connecting To A Hazelcast Cluster](https://youtu.be/-c5XPT4-daw?si=cmsLt13F3mXgnTca)

On the other hand, read on if you prefer reading some good ol' text.

#### Mandatory Properties

There are two things you have to tell Hazeltest if you want it to connect to a Hazelcast cluster: the cluster's name, and at least one endpoint to connect to. Both of these parameters are injectable via environment variables:

* ``HZ_CLUSTER``: Specifies the name of the target Hazelcast cluster (i.e. the string you specified on Hazelcast's side using the `hazelcast.cluster-name` property). For example, the Hazelcast Helm chart contained in this repository by default sets ``cluster-name`` to ``hazelcastplatform`` and, correspondingly, the Helm chart for Hazeltest sets ``HZ_CLUSTER`` to the same value.
* ``HZ_MEMBERS``: Although this can be a comma-separated list of members to connect to, in most cases, providing a single member -- or an endpoint pointing to at least one member, such as a loadbalancer host name -- is completely sufficient, as the all-member routing mode (also known as "smart routing") enabled by default in the Hazelcast Golang client used by Hazeltest will figure out the remaining members on its own and automatically connect to them, too, assuming they're on the same network and nothing blocks access. For example, if you run a Hazelcast cluster on a couple of VMs and want Hazeltest to load-test its members, specifying ``<host name or IP of one of the VMs>:5701`` should suffice, assuming you exposed the Hazelcast member running on the VM using Hazelcast's default port of ``5701``. (Obviously, if you're running your Hazelcast members on a system in which IPs have limited meaning because they can arbitrarily change -- think of a Pod running in Kubernetes --, you'll be better off using a hostname than an IP.)

#### Optional Properties

Although the following two properties are still important, they aren't mandatory for simply connecting to the target Hazelcast cluster, and because of that, they were modelled as command-line arguments rather than environment variables (you can find an example invocation setting both the former and the latter down below):

* ``-config-file``: Allows you to specify a custom configuration file, thus represents the means through which custom configuration for load creation behavior can be injected. To build your own configuration, I suggest you take the [``defaultConfig.yaml``](./client/defaultConfig.yaml) as a starting point and iteratively adjust configuration as required. Note that Hazeltest will take the default configuration from the aforementioned file for any property not explicitly overridden by means of a custom config file, so you only have to provide those properties in the latter that you wish to override.
* ``-use-unisocket-client``: Enables or disables usage of the uni-socket routing mode on the Hazelcast client. The default is the smart-routing (or all-member routing) mode (i.e. ``-use-unisocket-client=false``), but that mode only makes sense if all members of the target Hazelcast cluster can be reached (the classic example for when this isn't the case is when you're accessing Hazelcast members running on a different network with a loadbalancer acting as a single entrypoint into the set of members; in this case, uni-socket mode will establish and hold a sticky connection to whatever member of the cluster the loadbalancer happened to balance the initial request to).

#### Sample Invocation

So, let's assume you just downloaded the native Hazeltest executable from the [releases tag](https://github.com/AntsInMyEy3sJohnson/hazeltest/releases), you already wrote a custom configuration, and now you want to wreak havoc on an unsuspecting Hazelcast cluster!
 
The following is an example for how you can invoke the Hazeltest executable:

```bash
env HZ_CLUSTER=hazelcastplatform HZ_MEMBERS=192.168.44.129:32571 ./hazeltest-0.16.3-linux-arm64 -config-file=./my-custom-config.yaml -use-unisocket-client=true
```

This makes a couple of assumptions, of course, namely:
* You're on an ARM-based Linux system, so you downloaded `hazeltest-0.16.3-linux-arm64`
* Your Hazelcast cluster is called ``hazelcastplatform``
* There is a loadbalancer with the IP ``192.168.44.129`` whose port `32571` points to at least one Hazelcast member
* You run Hazeltest and the Hazelcast members on two separate networks (or network segments) and therefore have to rely on the sticky TCP connection unisocket client mode establishes to one of the Hazelcast members when initially making the connection via the loadbalancer

These are some assumptions that may or may not be true in your particular setup, so please treat this only as a suggestion, and adjust your invocatiob as needed.

## Diving Deeper

Interested in learning more about the concepts in Hazeltest whose workings you can observe on your Hazelcast clusters under test? Then look no further than the following sections.

### Underlying Idea
To shortly reiterate the application purposes stated previously, Hazeltest lets you load-test Hazelcast clusters, enabling you to iterate over the release candidates describing those clusters long before any of them gets rolled out to production. In this way, you can spot errors such as misconfigurations and identify improvement potentials for cluster/member performance, and not only address them long before the new release candidate thus created makes it to production, but also create proof that the fixes and improvements actually have the intended effect. Thus, you can establish proof that your release candidates are fit for production _before_ they actually get rolled out to production.

(Side note: If you're from the realm of software development, the idea described above -- you know, asserting production fitness for a given release candidate before deploying the thing to production -- probably appears as natural, and as self-evidently reasonable and useful, as the act of breathing! And that's because, well, it is. But, according to my own experience thus far, the release candidates describing middleware components -- such as databases, message queues, or a certain in-memory data and computing platform -- don't experience nearly the same amount of formalized, standardized testing than self-written code artifact release candidates (which strikes as a surprise given the criticality of the workloads such middleware components often run). It might be subject to wonderful discussion as to why this is the case, but either way, the absence of such testing is brave at best, and can entail catastrophic consequences at worst, hence Hazeltest is my attempt at solving this problem in the realm of Hazelcast.)

### Load Dimensions

Before we dive into how the application achieves the purpose outlined above, we need a framework within which the meaning of load for both the sender (the load-creating actor; here: the actors within Hazeltest) and the receiver (the Hazelcast cluster under test) can be established and then navigated more clearly and explicitly. As a suggestion for such a framework, I'm putting forward "load dimensions". 

Currently, there are six load dimensions (as far as the load Hazeltest can currently create is concerned; there are more dimensions to load a Hazelcast cluster can experience, hence as Hazeltest's feature set will be expanded, so will be the following list):

1. Number of items
2. Item size
3. Number of data structures
4. Number of clients
5. Cluster health
6. Operations per second

For example, a Hazelcast cluster could be under load in terms of these dimensions as follows (using only Hazelcast maps in this example):

1. 8 million items
2. Average size of 1.2 kb, with the largest items being up to 2mb in size
3. Total number of items distributed across 1.200 maps
4. Maps are operated on by 220 clients
5. The cluster is healthy (i.e. no member restarts, no members down due to maintenance, etc.) -- no additional load as a result of, for example, repartitioning operations
6. Across all maps, the members combined experience 5.000 sets, 2.000 puts, 11.000 gets, and 500 removes per second

It's important to classify load along those different dimensions due to the fact that, say, 8 million items in 1.200 maps will create totally different Hazelcast cluster and member usage and performance than, say, 8 million items in one large map, even if the total (net payload) memory consumed is exactly the same in both scenarios.

With a minimal framework for measuring and creating load established, it seems obvious that to fulfil its purpose, Hazeltest has to offer load-creating actors that are able -- not necessarily individually, but at least in combination -- to create load on these six dimensions.

Let's take a look at the load-creating actors available in Hazeltest and how you can get the most out of them.

### Map Runners And Map Test Loops
At the time of this writing, there are two Map Runners available in Hazeltest -- the Pokédex Runner and the Load Runner --, and they can be combined with two types of Test Loop, namely, the Batch Test Loop and the Boundary Test Loop. The relationship between Runners and Test Loops is one of parent-child, i.e. a Runner can use one type of Test Loop.

#### Runner vs. Test Loop

This begs the question, of course: Which component does what? As a rule of thumb, it can be said that Runners offer adjustability for load dimensions, whereas the Test Loop determines the kind and order of operation executed on the target Hazelcast cluster (except for load dimension 6, as both available Test Loops offer adjustability for sleeps, too, which translates to offering adjustability for the number of operations executed per second). In other words, the Runner as the framework around the Test Loop never executes any operations on Hazelcast by itself, but relies on the Test Loop to do so. 

Let's take a look at an example for how to configure the Map Load Runner/Batch Test Loop combination (many properties have been omitted here for brevity):

```yaml
mapTests:
  load:
    enabled: true
    # load dimension 3 (and, indirectly, load dimension 1)
    numMaps: 10
    # load dimension 1
    numEntriesPerMap: 50000
    payload:
      # load dimension 2
      fixedSize:
        enabled: true
        sizeBytes: 1200
    # load dimension 3 (impacts how the Runner will form map names, so 
    # translates to more or fewer maps)
    appendMapIndexToMapName: true
    # load dimension 3 (for the same reason)
    appendClientIdToMapName: false
    sleeps:
      # load dimension 6
      betweenRuns:
        enabled: true
        durationMs: 2000
        enableRandomness: true
    testLoop:
      type: batch
      batch:
        # load dimension 6 (only option to influence a load dimension on the 
        # test loop layer -- same goes for boundary test loop, although the
        # latter offers different kinds of sleeps)
        sleeps:
          afterBatchAction:
            enabled: true
            durationMs: 10
            enableRandomness: true
          afterActionBatch:
            enabled: true
            durationMs: 2000
            enableRandomness: true
```

So, except for load dimension 6, adjustability of load dimensions is offered exclusively by the Runner. The Test Loop, on the other hand -- although not explicitly expressed in terms of this declarative configuration, but rather as a result of the logic of the Test Loop itself -- decides when to execute which kind of operation on the Hazelcast cluster under test.

To drive this point home, consider the following diagrams:

![Comparison of Map Load Runner effects on maps in Hazelcast with Boundary Test Loop and Batch Test Loop](./resources/images_for_readme/map_load_runner_comparison_boundary_vs_batch_test_loop.png)

In the diagrams on the left-hand side and on the right-hand side, the Map Load Runner was configured in exactly the same way -- with the exception, as the heading indicates, of the Test Loop; the left half shows the Map Load Runner in combination with the Batch Test Loop, and the right displays the combination with the Boundary Test Loop. As you can see, the load creation outcome on the target Hazelcast cluster's maps varies significantly depending on the choice of Test Loop.

With the Map Runner and Test Loop concepts established and their relationship outlined, let's examine the available Runners and Test Loops more closely.

#### Pokédex Runner

The Pokédex Runner for creating load on maps is where it all started -- the first load creation mechanism in Hazeltest, which was implemented to observe how quickly the lite (or "compute") members of my client's Hazelcast clusters were able to respond to incoming `getMap` requests with different threading configurations when under high CPU load (the fact that today's Test Loops log the time it takes for their `getMap` calls to complete traces back to this first load creation use case). Requirements for this first load creation mechanism, then, were very simple:

1. Execute `getMap` calls and log time it took to complete each one
2. Keep CPU busy

It's for this reason I chose the first-generation Pokédex (the one you may remember if you had an awesome childhood and/or little interest to pay attention in school) as the basis for the first Runner (which then, obviously, became the "Pokédex Runner") -- a simple dataset with light-weight entries which the first iteration of Runner functionality used to execute batches of operations against the Hazelcast cluster under test. These operation batches -- ingest, read, delete -- could be run very quickly, particularly in combination with the light-weight Pokédex entries. Therefore, the Pokédex Runner made a great tool for measuring the duration of ``getMap`` calls on an empty cluster, stress the CPU, and then launch some additional instances to once again measure the time in which their ``getMap`` calls completed. Testing repeatability thus ensured, it was trivial to find the threading configuration for Hazelcast optimal for dealing with high volumes of map operations while maintaining low latencies for new applications' ``getMap`` calls. 

(In case you're suspecting now that the first iteration of functionality to run those operation batches eventually got pulled out of the Runner itself and became what's known today as the aforementioned _Batch Test Loop_ -- you're entirely correct! Indeed, the introduction of the Load Runner, which we're going to explore in the next section, necessitated a refactoring of the batch functionality into a dedicated concept with corresponding standalone code, so both resulting Runners could make use of it independently and with different configurations.)

Even in today's version of Hazeltest, if your goal is to simply stress the CPU of the target Hazelcast cluster as much as possible (and potentially measure operation times), the Pokédex Runner/Batch Test Loop combination will do a great job!

The following excerpt shows a possible configuration for the Pokédex Runner in combination with the Batch Test Loop (for explanations on those properties, please refer to the [`defaultConfig.yaml` file](./client/defaultConfig.yaml)):

```yaml
mapTests:
  pokedex:
    enabled: true
    # load dimension 3 (and, hence, albeit indirectly, load dimension 1)
    numMaps: 10
    # load dimension 3
    appendMapIndexToMapName: true
    # load dimension 3
    appendClientIdToMapName: false
    numRuns: 10000
    performPreRunClean:
      errorBehavior: ignore
      cleanAgainThreshold:
        enabled: true
        thresholdMs: 30000
    mapPrefix:
      enabled: true
      prefix: "ht_"
    # load dimension 6
    sleeps:
      betweenRuns:
        enabled: true
        durationMs: 2000
        enableRandomness: true
    testLoop:
      type: batch
      batch:
        # load dimension 6
        sleeps:
          afterBatchAction:
            enabled: true
            durationMs: 50
            enableRandomness: true
          afterActionBatch:
            enabled: true
            durationMs: 5000
            enableRandomness: true
```

You may have spotted the `performPreRunClean` configuration object as hint for a concept in Hazeltest that hasn't been formally introduced yet, but don't worry about it for now, we're going to give this feature its due introduction later on.

In terms of the aforementioned load dimensions, this is what the Pokédex Runner offers: 

1. Number of items: Adjustable only indirectly (because fixed dataset) by increasing the number of maps or the number of Runners (by adding more Hazeltest instances)
2. Item size: Not adjustable
3. Number of data structures: By means of the ``numMaps`` property, or by adding more Hazeltest instances
4. Number of clients: Not adjustable on the Runner itself, but by adding more Hazeltest instances
5. Cluster health: Not adjustable
6. Operations per second: By means of ``sleeps.betweenRuns`` and the sleep configurations of the chosen test loop

In other words: As the preceding descriptions of the feature indicated, the Pokédex Runner is not an appropriate tool to load-test a Hazelcast cluster's abilities to keep payloads in memory, but it's excellent at stressing the CPU. For load-testing the in-memory storage capacities of a Hazelcast clusters, the Load Runner is the far better tool.

#### Load Runner
The Load Runner enables you to optimize load creation along load dimensions 1 and 2, that is, the number of items and the size of each item stored in the Hazelcast cluster under test, respectively, which is why it's the bread-and-butter feature in Hazeltest for load-testing a Hazelcast cluster's ability to keep payloads in memory and serve them from there.

In case you don't really feel like reading an awful lot of text right now and prefer a video instead, go check out this video on my channel, which provides an introduction to the Load Runner:

[Let There Be Load! - Part 1: An Introduction To The Map Load Runner](https://youtu.be/I15GnJJGKf4?si=0YXPT-VSVx38F4ka)

In order to offer adjustability of load dimensions 1 and 2, the Load Runner doesn't work on a fixed dataset, but creates a random string payload according to the desired specifications. Therefore, the Load Runner's configuration comes with some additional properties, as the following complete example configuration making use of the Batch Test Loop highlights (again, the [`defaultConfig.yaml` file](./client/defaultConfig.yaml) has explanations on all properties in store for you):

```yaml
  load:
    enabled: true
    # load dimension 3 (and, indirectly, load dimension 1)
    numMaps: 10
    # load dimension 1 
    numEntriesPerMap: 50000
    # load dimension 2
    payload:
      fixedSize:
        enabled: false
        sizeBytes: 10000000
      variableSize:
        enabled: true
        lowerBoundaryBytes: 1000
        upperBoundaryBytes: 10000
        evaluateNewSizeAfterNumWriteActions: 100
    # load dimension 3
    appendMapIndexToMapName: true
    # load dimension 3
    appendClientIdToMapName: false
    numRuns: 10000
    performPreRunClean:
      enabled: false
      cleanMode: destroy
      errorBehavior: ignore
      cleanAgainThreshold:
        enabled: true
        thresholdMs: 30000
    mapPrefix:
      enabled: true
      prefix: "ht_"
    # load dimension 6
    sleeps:
      betweenRuns:
        enabled: true
        durationMs: 2000
        enableRandomness: true
    testLoop:
      type: batch
      batch:
        # load dimension 6
        sleeps:
          afterBatchAction:
            enabled: true
            durationMs: 10
            enableRandomness: true
          afterActionBatch:
            enabled: true
            durationMs: 2000
            enableRandomness: true
```

While most of the configuration is identical to that of the Pokédex Runner, the Load Runner takes the following additional properties:
* The ``numEntriesPerMap`` property, which offers adjustability of load dimension 1
* The entire ``payload`` configuration object, offering, by means of its various sub-properties, configuration of load dimension 2

Here, the two options available for payload generation deserve some additional explanation:
* __Fixed-size payload generation mode__ (``payload.fixedSize``): Will have the Load Runner create one random string according to the given size-in-bytes configuration (yes, the value for this property will contain quite a few zeros for payloads in the megabyte range) to work with on the target Hazelcast maps. So, nothing surprising there, really, but things get more interesting with the...
* ... __variable-size payload generation mode__ (``payload.variableSize``): Turns out both Hazelcast and the JVM itself aren't that good with handling variable-size payloads (translating internally to heap objects with variable sizes, or variable-size payloads in Hazelcast native memory, if you happen to use the Enterprise Edition) -- in fact, crashing even well-configured Hazelcast members is definitely possible if they have to deal with payloads exhibiting significant variation in their sizes (e.g. from kilobytes to megabytes). Thus, if you already know the client applications that will access your production Hazelcast clusters create variable-size payloads, it's probably a good idea to assert that your cluster configuration is able to deal with those size variations! It's for this reason that variable-size payload generation mode lets you specify a lower boundary and an upper boundary (`lowerBoundaryBytes` and `upperBoundaryBytes`, respectively) for the payload size, which comes in very handy when you know at least roughly the size of the smallest and the largest payloads, and want the Load Runner to simply create payloads with random sizes in that range, without caring so much about the precise size of each individual map item created. The third property in the bunch, then, `evaluateNewSizeAfterNumWriteActions`, is simply a little optimization, so a new random payload doesn't have to be created for every write action (unless you explicitly set this property to `1`).

As you'd expect after the preceding sections, the Load Runner offers adjustability of load dimensions as follows:

1. Number of items: By means of ``numEntriesPerMap``
2. Item size: By means of the ``payload`` configuration object and its various sub-properties
3. Number of data structures: By means of the ``numMaps`` property, or by adding more Hazeltest instances
4. Number of clients: Not adjustable on the Runner itself, but by adding more Hazeltest instances 
5. Cluster health: Not adjustable
6. Operations per second: By means of ``sleeps.betweenRuns`` and the sleep configurations of the chosen test loop

So, in combination with the option of adding more Hazeltest instances, the Load Runner covers all load dimensions except dimension 5 (unless you configure it so it crashes some Hazelcast members, in which case cluster health is obviously affected) and is therefore much more flexible than the Pokédex Runner in terms of the use cases it can cover -- stressing the CPU (previously the domain of the Pokédex Runner) is just as easily doable as exhausting gigabytes or even terabytes of memory. 

(This begs the question, of course, of why the Pokédex Runner is still around. Well, it still has its simplicity going for it -- if your goal is to simply stress the CPU of unsuspecting Hazelcast members, the Pokédex Runner is perfectly sufficient, but saves you the hassle of worrying about adjusting payload sizes and the number of map items to create, so you can get started more easily.)

#### Map Runner/Test Loop Combinations
As explained above, the Map Runners themselves don't actually execute operations on the Hazelcast cluster under test -- it is, instead, the Test Loop the Runner has been configured with that does. So, if we're any serious about creating appropriate load on a target Hazelcast cluster -- "appropriate" in the sense that the generated load satisfies the requirements we have for our load tests --, we need to take a short look at the available Test Loops and what use cases they can address in combination with an enveloping Runner.

Before we get to that, however, let's take a short look at the Test Loops that are available as of this writing, and their characteristics:

* __Batch Test Loop__: Runs batches of operations (ingest all, read all, delete some). 
* __Boundary Test Loop__: Offers configuration of a lower and an upper boundary for the fill level of a target map, and the Test Loop will perform operations on the target map such that these boundaries are honored. Thus, the Boundary Test Loop simulates the usage pattern created by real-world applications that are subject to the time-of-day, in which load is almost zero at night, rises during morning hours and reaches its peak around noon, after which the pattern reverses and traffic declines until it's down to almost nothing again during the night, so the pattern then repeats throughout the next day.

In other words, both Runners available as of this writing can be combined with either the Batch Test Loop or the Boundary Test Loop.

##### Runner Feat. Batch Test Loop: Quick And Dirty Smoke Testing

The implementation of the Batch Test Loop is _extremely_ simple due to the fact that it literally just has to run batches of ingesting all elements, reading them back out again, and then deleting some of them, in order to then start with the next iteration of batches. Obviously, the kind of load generated on the target Hazelcast cluster as a result isn't exactly realistic (which kind of real-world client would ingest all elements, read all of them, and then delete some of them, only to then begin anew?), but as it turns out, during the early phases of testing a release candidate (e.g. a Helm chart describing a Hazelcast cluster), the realism of generated load is a lot less important -- fades into insignificance, really -- than minimizing the time-to-feedback (feedback on the fitness of the release candidate, that is).

Basically, the idea is this: In scope of the CI/CD pipeline that builds and deploys your release candidate, there should be a short, automated smoke test to assert a basic level of fitness, and if it passes the fitness threshold thus established, you can move it to higher stages, where more sophisticated testing (the kind of testing where load creation realism does actually matter) can be performed. Of course, if the release candidate _doesn't_ make that initial fitness hurdle, another iteration has to be performed on it, probably altering some configuration, effectively creating a new release candidate, which is then also sent into the CI/CD pipeline, where it becomes subject to the same automated smoke testing. 

What's important here is _time_ or, more precisely, the time it takes for you, the engineer, to get feedback on the fitness of the release candidate you've just built: The smaller the time span between _commit made_ and _smoke test performed_, the more iterations you can do in one unit of time (one typical workday, for example) -- or the quicker you can move on to higher (and more fun) stages, if the initial smoke test was passed and no further iterations are necessary at this point. Another way to look at this would be to say that the quicker you can perform smoke testing, the more value your organization gets out of your work -- hence, minimizing the time-to-feedback translates to a very much tangible, real-world advantage for your organization. 

You might object here that while a smoke test runs, you can simply perform other tasks, and that, perhaps, you don't care about how much value your organization gets out of your work, because all you've ever received as rewards for the value you're creating was the occasional free banana, or maybe a pizza on the house, if you were really kicking it. So, if you're simply asking yourself why you would bother investing effort into minimizing that time-to-feedback, that's totally fair enough! What's in it for you, then? Well, potentially two things:

1. You might go on to do other things while a test of any length is running, but that destroys your focus -- that precious flow state you may have worked yourself into. Let that happen a couple of times throughout your work day and I bet you'll feel exhausted and unproductive at the end of said day, without really knowing why. To put the emphasis on the positive aspect: Building and retaining a focused state of mind, potentially even that aforementioned flow state, is important to feel productive at the end of the work day. Consequently, you don't build up and maintain that focus for your employer, but _for yourself_ -- the fact that your employer also likes the idea of you being focused and in flow state is a side effect you don't really have to care about. So, who gives a damn about that bloody free banana? It's about the value _you yourself_ are getting out of your work, so really, you're doing the work for yourself, your own sense of accomplishment, your own mental well-being, and your own personal path of "aiming upward". Which brings us to the next point.
2. It is all too easy to succumb to the resentment that is inevitably the result of working hard and feeling you're receiving too little in terms of compensation. "Look at [insert name of obnoxiously lazy coworker here]", you might cry out, "he/she is doing jackshit and our salaries are almost the same!". Undoubtedly, there are injustices in the system, but if you use those as an excuse not to give your best, you'll inevitably replace your upward aim with a downward aim -- and that's a path to personal hell, the path to being your worst possible self. Aiming downward -- as you inevitably do if you don't aim upward, because the world around you keeps evolving, and without properly adjusted upward aim, you'll simply get overtaken and become worse just because others have become a little better -- is, of course, covered by the distortion of Free Will, so if you really wish to do that, knock yourself out! It seems to me, however, that if you're reading this, you know that the alternative, namely, aiming upward in a proper manner is infinitely more desirable, because it will inevitably lead you on a path of constant self-improvement, a path of giving your best no matter what. And that's also -- and also inevitably -- a path of taking full responsibility, of never looking for excuses, of being in alignment with some higher calling you probably won't be able to quite put your finger on, but it's there -- and once you follow the path of that calling, the universe will open unexpected doors for you, and you will become an unstoppable force -- a fully self-responsible, self-effective individual going to bed every night knowing you've given the world -- and yourself! -- the very best you had to offer, and that's a great gift indeed. Screw that occasional free banana -- those feelings of inner satisfaction and mastery over yourself are infinitely more valuable than the weight of a billion bananas compensated in gold.

(Wow, that was a lot of words for saying "Hey, try out the Runner/Batch Test Loop combination, it's really cool", but I hope you enjoyed the read!)

With the importance of automating release candidate smoke testing (where no automation is already present) and minimizing time-to-feedback (where that time isn't already optimal) as an act of aiming upward and a very practical means to retain your focus and clarity of mind sufficiently highlighted (I hope), let's explore why the combination of Load Runner and Batch Test Loop is a great fit for this purpose -- and why the Pokédex Runner/Batch Test Loop combination isn't.

We'll give some attention to the latter aspect first. As you might recall from the section on the Pokédex Runner, this load creation tool is great for stressing the CPU, but far less so for exhausting memory due to the absence of adjustability of load dimensions 1 and 2. In scope of a smoke test, however, you'll typically want to test the ability of the Hazelcast cluster spawned by the release candidate to keep payloads in memory, and remain stable and performant even when under high load, both in terms of CPU and _in terms of memory_ -- after all, that's what Hazelcast is all about: Keeping stuff in memory! Hence, the Pokédex Runner simply isn't the tool for the job.

Quite the contrary can be said about the Load Runner, however. It has the downside of being somewhat more complex to configure because it needs some bells and whistles to offer adjustability of load dimensions 1 and 2, but what you get in exchange if you combine it with the Batch Test Loop is a tool that can ingest lots of data into a Hazelcast cluster under test _very_ quickly. Therefore, if you wish to exhaust memory quickly in scope of a load test in order to verify that the members of the cluster under test remain stable, the Map Load Runner/Batch Test Loop combination is a perfect fit!

The following video demonstrates how the Map Load Runner/Batch Test Loop combination can be used to expose flaws in a Hazelcast cluster configuration by using the example of memory configuration. As you'll see, each iteration performed on the memory configuration as a result of a smoke test having exposed a flaw in the previous configuration leads to more stable members:

[Let There Be Load! - Part 2: Map Load Runner Feat. Batch Test Loop](https://youtu.be/QCrz5DZa6rc?si=7SsGKm-LUQLAWd2T)

Basically, the idea of performing smoke tests with the Load Runner/Batch Test Loop combination is this:

1. Get a rough estimate for the average size of the payloads you'll want to ingest in scope of a smoke test. As mentioned above, in this very early stage of testing, the load generation patterns don't have to be realistic, but the payload sizes should be at least somewhat close to what the Hazelcast cluster spawned by the release candidate once deployed to production will have to work with (you could set the payload size to 20mb in the Load Runner and exhaust all your memory in less than five seconds, but it's pointless optimizing for this kind of payload size only to discover that your production cluster has to deal with payloads 10kb in size at most).
2. Based on that size estimate and on how much memory the cluster to be smoke-tested has at its disposal, you can calculate how many items the Load Runner/Batch Test Loop combination will have to write, and configure the corresponding properties accordingly. For example, if your smoke test needs to fill 48gb of memory as quickly as possible, and you estimated the average payload sizes to be 2kb, you could start with ``numEntriesPerMap: 400000`` (load dimension 1), ``payload.fixedSize.sizeBytes: 2000`` (load dimension 2), and ``numMaps: 30`` (load dimension 3), which will fill the memory nicely assuming a backup count of 1 on the map pattern.
3. Run your smoke test and measure the time it takes for the memory of the cluster under test to be completely exhausted. If the time-to-feedback is too large, you'll want to optimize along load dimension 6 -- the number of operations per second -- to fill that memory more quickly. You can either give your Hazeltest instance more CPU or scale out horizontally. Increasing along load dimension 3 is also possible, but using a higher number of maps will spawn more Goroutines internally, so you'll encounter diminishing returns when setting too high a number. Where exactly the "sweet spot" between the level of parallelism and too much context switching lies is a matter of experimentation, but in the tests I've run on my servers so far, performance in terms of load dimension 6 using 2 CPU cores was optimal with 30 maps, and it's for this reason that ``numMaps: 30`` was provided as an example value above. Generally speaking, a smoke test shouldn't run longer than 2 minutes, and that includes the Batch Test Loop's read and delete batches, so the entire ingest phase shouldn't take longer than 40 seconds. (Remember that we want to optimize the time-to-feedback, so you can run smoke tests quickly in order to retain your focus and remain in that valuable flow state!)
4. Once your smoke test fills the Hazelcast cluster under test reasonably quickly, you can start observing the Hazelcast members for their performance and their stability. Stuff to look out for could, for example, be the following: What does the heap usage look like during the ingest phase before and while the memory is at maximum capacity? In particular, are there any signs of garbage collections not being able to free enough memory, so heap usage increases over time? If you have WAN replication enabled, what's the performance on the publishers like -- can they cope with the load? What is the average latency of operations on the map in use by the Load Runner's Batch Test Loops -- are there spikes in there, particularly once eviction starts? If you're on Kubernetes, how much buffer is there still left between the container's memory usage and its configured limit -- is the Pod's Hazelcast container in any danger to get restarted by the OOMKiller?  
5. If all cluster metrics are in the green, great! That means you've successfully established that the release candidate that brought forward this cluster satisfies a basic level of fitness, and that you can move it to higher stages, where the Map Load Runner/Boundary Test Loop might come in handy, see next section. If not -- well, congratulations (unironically), you successfully spotted a misconfiguration in that release candidate -- one that might have entailed catastrophic consequences had it been shipped to production, and ruined everyone's day. So, at this point, both outcomes are equally good, really. If a misconfiguration is present, you can do another iteration on your release candidate, effectively creating a new one, and send it through the smoke testing again.
6. Reevaluate the level of fitness you want your smoke test to establish. Your new release candidate is able to handle the load a previous one wasn't? That's great! But what if you had the Load Runner/Batch Test Loop combination spawn more maps, or if the payloads were just a tad larger...? 

As a final remark, note that you'll typically want an eviction policy in place that starts evicting when, say, 85 % of native memory is occupied, so the net memory you can effectively use is a little lower than the member's raw capacity. Other factors, such as the metadata space when using Hazelcast's native memory, might reduce the available net storage further. Aiming for the raw capacity is still a good idea, though -- after all, we want to assert the cluster members remain stable even when having reached maximum capacity and further requests come in.

##### Runner Feat. Boundary Test Loop: Modelling And Creating Production-Like Load

So, your release candidate passed the basic fitness threshold established by the Load Runner/Batch Test Loop combination, which now performs automated smoke testing for every run of your CI/CD pipeline? That's great -- congratulations! What's next?

As was hinted at above, the closer your release candidate inches to production through the various stages your corporation may have laid out, the more important the degree of realism of the load generated by a load test becomes -- after all, once a basic fitness threshold has been established by means of the aforementioned smoke test, you want to make sure your release candidate is able to handle the load it will face in production, rather than just _any_ load.



### Queue Runners

#### Tweets Runner

#### Load Runner

### Other Concepts 

#### Chaos Monkeys

#### State Cleaners

### Configuration

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
