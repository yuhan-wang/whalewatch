# Whale Watch
Detect real-time market manipulation for the cryptocurrency market by examining the order-book

## Table of Contents
1. [Motivation](README.md#motivation)
1. [Approach](README.md#approach)
1. [Running Instructions](README.md#running-instructions)
1. [Assumptions](README.md#assumptions)
1. [File Overview](README.md#file-overview)
1. [Detection Algorithm](README.md#detection-algorithm)
1. [To-Do's](README.md#to-do)
1. [Contact Information](README.md#contact-information)

## Motivation
The cryptocurrency market is rapidly expanding: as of 06/23/2020 had a market capitalisation of around 275 billion US dollars ([CoinMarketCap](coinmarketcap.com)) making it comparable to the GDP of Finland. Despite the vast amounts of money being invested and traded into cryptocurrencies, they are largely unregulated and as a result become a hot bed for price manipulation. While there are numerous types of market manipulations, a common practice performed by the bad actors is whale trading, where traders with deep pocket (a.k.a whales) place orders of enormous size to swing the prices in their favor.

This project aims at building a monitor system to identify orders with anomalous sizes by streaming live order book data from 2 cryptocurrency exchanges Bitfinex and Binance. The users can then take appropriate actions knowing that there might be artificial price movements.

## Approach
$fig to be added
1. By using the websocket API packages provided by Bitfinex and Binance,we first ingests (real-time or per 100ms) order book data of all trading pairs available for the spot market into the pipeline.
2. Kafka producers use the data to maintain a local order book as well as calculate various statistics before sending the enhanced messages to the topic `all`
3. Spark Structured Streaming executors consumes the data and filtered the new orders and compute a metric called `whale_score` to be used to identity whales before sending the data to the PostgreSQL database `order_books`.
4. The Dash front-end continuously submit queries to the PostgreSQL server to be executed. The database executes the queries and returns the results back to the front-end, which displays the (whale and normal) orders on the monitor website.

## Running Instructions
* Spin up a Kafka cluster, a Spark cluster, a PostgreSQL server and a Dash front-end server.
* Install the Kafka and Zookeeper, Spark and Hadoop, PostgreSQL binaries on the corresponding instances
* Install the jdbc-postgreql-driver binary on the spark master
* Install the Python packages: `kafka-python` `bitfinex-api-py` `unicorn-binance-websocket-api` on the Kafka Cluster, `pyspark` on the Spark cluster, `dash` `plotly` `psycopg2` `pandas` on the front-end server
* Start Zookeeper and Kafka, HDFS and Spark on the Kafka and Spark clusters respectively
* Make sure the PostgreSQL server is running and listening to all. Also create a databse called `order_books` and two tables `all_updates` and `new_orders`.
* Make sure the `spark-default.conf` file is modified to add the `postgreql.jar` installed to the (spark) driver extraClass
* Clone this repo to all instances
* Modify the hosts, username and passwords in the producer scripts ``binance.py`` ``bitfinex.py``, spark script ``orders.py`` and front-end script ``visualization.py``.
* Run producer scripts on Kafka instance(s) (the same producer should be on one node only)
* Submit Spark job on the master instance of the Spark cluster 
* Specify the host and run the Dash front-end script
* Open the host address on a browser

## Assumptions
* The (job) log retention policy used by spark ensures the size of the logs will not exceed the disk size of the instances. 
* The log retention used by kafka ensures the size of the logs will not exceed the disk size of the instances. (Default is 7 days, change it accordingly if disk space is limited.)
* The PostgreSQL server has enough disk space. The input size is roughly **3.75GB/hour**.
* Each Spark cluster has a Hadoop Distributed File System with a `/ckpt` directory
* The lists in `trading_pairs/` contain all the currency pairs traded at the exchanges (may not be true after several months)

## File Overview
``bin/`` contains all the scripts the pipeline uses.

### binance.py

#### description: 
This is the kafka-producer script for the exchange **Binance.com**. It creates a connection to the exchange's Websocket API, subscribes to the **depth@100ms** chanel for all currency pairs. It also maintains the local order books for the trading pairs and computes the **change** in order size as well as various statistics that relies on knowledge of the full order book before publishing them to the topic `all`. Each individual order book extends the ``OrderBook`` class defined in the ``order_book.py``.


### bitfinex.py

#### description: 
This is the kafka-producer script for the exchange **Bitfinex**. It creates a connection to the exchange's Websocket API, subscribes to the **book** chanel for all currency pairs. It also maintains the local order books for the trading pairs and computes the **change** in order size as well as various statistics that relies on knowledge of the full order book before publishing them to the topic `all`. Each individual order book is an instance of the ``OrderBook`` class defined in the ``order_book.py``.

### order_book.py

#### description:
This file contains two items: a function ``kafka_send`` that converts and serializes the order data of a currency pair and publishes to the input topic and a ``OrderBook`` class with methods necessary to maintain an order book and produce the statistics. 

### orders.py

#### description: 
This is the spark processor script. It consumes the messages from the topic ``all`` and sends the raw messages directly to the PostgreSQL sink. It then generates the statitics and uses them to compute a discrete metric called **whale_score** on a scale from 0 to 6 with 6 being the mostly likely to be a whale and 0 being the least. 

### visualization.py

#### description: 
This is the Dash/Plotly script that runs the front-end. It illustrates in real-time the orders as bubbles with size indicating the quantity and color indicating the side and the whale_score in a scatter ploy with price as the y-axis and time as the x-axis. The exchange and currency pairs can be selected from the drop-down lists. There is also a button that controls the streaming.

## Detection Algorithm

The metric **whale_score** used to detect whales is computed as follows:
Assume the order sizes say ``q`` follow a lognormal distribution. Transform ``q`` into ``q'`` so that ``q'`` is normally distributed. Then
1. If ``q'`` is ``k``-stanard-deviation bigger than the average, it gets a **whale_score** ``k-1`` for k=2,3,4
2. Add 1 to **whale_score** if the quoted spread is bigger than 1%.
3. Add 1 to **whale_score** if the price of the order is within 1% of the midpoint (of the best-ask and best-bid)
4. Add 1 to **whale_score** if the size is more than 10 times the order imbalance (difference in the sizes of open bids and asks)

## To-Do's
1. The codes can be further cleaned and modualarized:
* Instead of modifying the scripts to change the hosts and SQL server username and password, use system arguments to supply them. 
* The computation for the metric **whale_score** is hard-coded. It should allow users to apply different models to compute the metric. For example, a different prior distribution.

2. Support more exchanges:
* Either use third-party packages such as [cryptofeed](https://github.com/bmoscon/cryptofeed)
* Or write more producer scripts

3. Decouple the mointor system and raw data storage to improve performance
* Instead of using the PostgreSQL server sink, use Kafka as a sink and then the front-end works as a consumer
* The raw order data can still be sent to the databse server

## Contact Information
* [Yuhan Wang](https://www.linkedin.com/in/wangyuhan/)
* wangyuhan@live.cn
