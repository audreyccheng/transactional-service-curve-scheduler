# Transactional Service Curve Scheduler

## Extended Version of Paper
We include the extended version of our paper (Appendix) as a PDF in the root repository.

## Code
This repository contains code implementing Transactional Servce Curve Scheduler (TSCS), a transactional scheduling policy that enforces transactional service curves.

This repository contains:
1. TSCS, which modifies [RocksDB](https://rocksdb.org/) to add our scheduling policy
2. Various transactional scheduling baselines, including SMF, DRFT, and FIFO
3. Benchmarking code for the paper with benchmarks based on [OLTPBench](https://github.com/oltpbenchmark/oltpbench) and [TAOBench](https://taobench.org/)

This repository is structured as follows:
- /benchmarks - the application benchmarks tested on TSCS
- /rocksdb - the modified version of RocksDB supporting transactional scheduling

Prerequisites:
- mvn 3.8.5
- build-essential
- Java 17
- C++17 required (GCC >= 7, Clang >= 5)

Note: if running on EC2 or other cloud providers, make sure security groups / firewalls allow all traffic.

## TSCS
To run benchmarks against TSCS, start RocksDB and the benchmark code on separate machines. The benchmark starts up worker threads that send transactions over the network to the machine hosting TSCS. These requests are received by a database proxy, which then redirects the transactions to TSCS. We mainly modify /rocksdb/utilities/transactions and /rocksdb/java since the benchmark is in Java.

To run a benchmark:

1. `cd benchmarks`

2. `mvn clean install`

3. `mvn compile assembly:single`

4. Configure benchmark client: `____ExpConfig.json` (examples available in /benchmarks/configs)

    - `exp_length`: experiment length in seconds

    - `ramp_up`: warm-up time in seconds

    - `must_load_keys`: whether loading phase should run

    - `nb_loader_threads`: number of threads during loading phase

    - `n_worker_threads`: number of threads available to the benchmark

    - `nb_client_threads`: number of threads available to generate transactions

    - `n_receiver_net_threads`: number of threads to receive network messages

    - `n_sender_net_threads`: number of threads to send network messages

    - `node_ip_address`: IP address of benchmark machine

    - `proxy_ip_address`: IP address of TSCS machine

    - `proxy_listening_port`: post on which TSCS machine is listening for client benchmark requests

    - `must_schedule`: turn on / off scheduler

5. To run loader and benchmark client: `java -jar ___.jar ___ExpConfig.json`

Note: to build a different jar, edit `pom.xml` plugin.

To build TSCS jar:

1. `mkdir build; cd build`

2. `cmake ..`

3. `cd ..`

3. `make DEBUG_LEVEL=0 rocksdbjava`

To run TSCS and database proxy, add TSCS build file to jar of database proxy:

1. `cd benchmarks`

2. `mvn install:install-file  -Dfile=TSCS jar> -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=1 -Dpackaging=jar -DgeneratePom=true`

3. `mvn compile assembly:single`

4. Configure database proxy: `____ExpConfig.json` (example available in /benchmarks/configs)

    - `n_worker_threads`: number of threads available to the proxy

    - `nb_client_threads`: number of threads available to send requests over the network

    - `n_receiver_net_threads`: number of threads to receive network messages

    - `n_sender_net_threads`: number of threads to send network messages

    - `node_ip_address`: IP address of benchmark machine

    - `proxy_ip_address`: IP address of TSCS machine

    - `proxy_listening_port`: post on which TSCS machine is listening for client benchmark requests

    - `useproxy`: whether or not proxy should be used

    -  `delete_db`: delete contents of RocksDB

5. To start TSCS and database proxy: `java -jar ___.jar ___ExpConfig.json`
