# Hazeltest
Welcome, fellow Hazelcast warrior! If you're looking at this code repository, it may be so because you've been facing the challenge of testing your Hazelcast clusters, and maybe you've been wondering if there are tools out there making that process a bit easier.

Hazeltest is an application that attempts to achieve just that: By means of simple-to-configure, yet effective and versatile test loops, it stresses a Hazelcast cluster, thus facilitating the discovery of misconfigurations or other errors.

Right now, the application is still in a very early phase of development, but because I'm using it as a real-world project for learning Golang -- and because I actually do face the challenge of testing Hazelcast clusters in a project I currently work in --, you can expect more features to be added in the upcoming weeks.

For a general overview of the background and ideas behind Hazeltest, please refer to the introductory blog post I've written, which you can find [here](https://nicokrieg.com/hazeltest-introduction.html).

## Getting Started
Have a Kubernetes cluster at your disposal? Then you're in luck, because the easiest and most convenient way to get started is to apply the two Helm charts you can find in this repository's [chart](./resources/charts/) folder to it. First, get yourself a neat little Hazelcast cluster by running the following:

```bash
$ helm upgrade --install monitoredhazelcast ./monitoredhazelcast --namespace=hazelcastplatform --create-namespace
```

Once the cluster is up and running, you can install Hazeltest like so:

```bash
$ helm upgrade --install hazeltest ./hazeltest --namespace=hazelcastplatform
```

In the Hazeltest pod, you should see some logging statements informing about the duration of a bunch of `getMap()` calls the two runners enabled by default have made on the Hazelcast cluster.

## Diving Deeper
The next step you may be inclined to do is to take a closer look at which test runners are currently available in the application and how they can be configured.

### Available Runners
The first runner available today is the `PokedexRunner`, which runs the test loop with the 151 Pokémon of the first-generation Pokédex. It serializes them into a string-based Json structure, which is then saved to Hazelcast. The `PokedexRunner` is not intended to put a lot of data into Hazelcast (i. e., it is not intended to load-test a Hazelcast cluster in terms of its memory), but instead stresses the CPU. The second available runner, on the other hand, is the `LoadRunner`, and as its name indicates, it is designed to "load up" the Hazelcast cluster under test with lots of data such as to test the behavior of the cluster once its maximum storage capacity has been reached. As opposed to the `PokedexRunner`, which is -- by nature of the data it works with -- restricted to 151 elements in each map, the `LoadRunner` can be configured arbitrarily regarding the number of elements it should put into each map, and the elements' size is configurable, too. 

### Configuration
The default configuration resides right with the source code, and you can find it [here](./client/config/defaultConfig.yaml). It contains all properties currently available for configuring the two aforementioned runners along with comments shortly describing what each property does and what it can be used for.

You can find all properties for configuring the Hazeltest application itself in the [`values.yaml`](./resources/charts/hazeltest/values.yaml) file of the [Hazeltest Helm chart](./resources/charts/hazeltest/) along with comments explaining them.



