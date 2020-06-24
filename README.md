# Whale Watch
Detect real-time market manipulation for the cryptocurrency market by examining the order-book

## Table of Contents
1. [Motivation](README.md#motivation)
1. [Approach](README.md#approach)
1. [Running Instructions](README.md#running-instructions)
1. [Assumptions](README.md#assumptions)
1. [Files in Repo](README.md#files-in-repo)
1. [Encryption](README.md#encryption)
1. [Scalability](README.md#scalability)
1. [Future Work](README.md#future-work)
1. [Contact Information](README.md#contact-information)

## Motivation
The cryptocurrency is rapidly expanding: as of 06/23/2020 had a market capitalisation of around 275 billion US dollars (CoinMarketCap) making it comparable to the GDP of Finland. Despite the vast amounts of money being invested and traded into cryptocurrencies, they are largely unregulated and as a result become a hot bed for price manipulation. While there are numerous types of market manipulations, a common practice performed by the bad actors is whale trading, where traders with deep pocket (a.k.a whales) place orders of enormous size to swing the prices in their favor.

This project aims at building a monitor system to identify orders with anomalous sizes by streaming live order book data from 2 cryptocurrency exchanges Bitfinex and Binance. The users can then take appropriate actions knowing that there might be artificial price movements.

## Approach
$fig to be added
1. By using the websocket API packages provided by Bitfinex and Binance,we first ingests (real-time or per 100ms) order book data of all trading pairs available for the spot market into the pipeline.
2. Kafka producers use the data to maintain a local order book as well as calculate various statistics before sending the enhanced messages to the topic `all`
3. Spark Structured Streaming executors consumes the data and filtered the new orders and compute a metric called `whale_score` to be used to identity whales before sending the data to the Postgresql database `order_books`.
4. The Dash front-end continuously submit queries to the Postgresql server to be executed. The database executes the queries and returns the results back to the front-end, which displays the (whale and normal) orders on the monitor website.

## Running Instructions
* Spin up a Kafka cluster, a Spark cluster, a Postgresql server and a Dash front-end server.
* Install the Kafka, Spark, Postgresql binaries on the corresponding instances
* Install the Python packages and other system requirements.
* Clone this repo to all master instances
* Run producer scripts on Kafka instance(s) (the same producer should be on one node only)
* Submit Spark job on the master instance of the Spark cluster 
*
