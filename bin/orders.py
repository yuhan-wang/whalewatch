import math

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import IntegerType

dbname = "order_books"
username = "postgres"
password = ""
kafka_host = "PLAINTEXT://ip-10-0-0-13.ec2.internal:9092"
db_host = "ip-10-0-0-11.ec2.internal"
hadoop_host = "ip-10-0-0-8.ec2.internal"


def write_batch(batch, batchId, host, username, password, dbname, tablename, portnum=5432):
    '''
    save the result table in postgres
    '''

    url = "jdbc:postgresql://{}:{}/{}".format(host, portnum, dbname)
    properties = {"user": username, "password": password, "driver": "org.postgresql.Driver"}
    batch.write.jdbc(url=url, table=tablename, mode='append', properties=properties).save()


def writeBatchAll(batch, batchId):
    return write_batch(batch, batchId, db_host, username, password, dbname, "all")


def writeBatchNew(batch, batchId):
    return write_batch(batch, batchId, db_host, username, password, dbname, "new_orders")


spark = SparkSession \
    .builder \
    .appName("orders") \
    .getOrCreate()
df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafka_host) \
    .option('subscribe', 'all') \
    .load() \
    .selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)', 'timestamp AS time')
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df = df \
    .selectExpr(
    'time',
    'SPLIT(key, ",")[1] AS basequote',
    'SPLIT(key, ",")[0] AS exchange',
    'CAST(SPLIT(value, ",")[0] AS DOUBLE) AS price',
    'CAST(SPLIT(value, ",")[1] AS DOUBLE) AS count',
    'CAST(SPLIT(value, ",")[2] AS DOUBLE) AS quantity',
    'CAST(SPLIT(value, ",")[3] AS INT) AS side',
    'CAST(SPLIT(value, ",")[4] AS DOUBLE) AS best_bid',
    'CAST(SPLIT(value, ",")[5] AS DOUBLE) AS best_ask',
    'CAST(SPLIT(value, ",")[6] AS DOUBLE) AS active_bids',
    'CAST(SPLIT(value, ",")[7] AS DOUBLE) AS active_asks',
    'CAST(SPLIT(value, ",")[8] AS DOUBLE) AS avg',
    'CAST(SPLIT(value, ",")[9] AS DOUBLE) AS var',
) \
    .withWatermark('time', '1 minute')
# send dataframe containing filled and canceled to storage
# to do
##
allQuery = df \
    .writeStream \
    .option('checkpointLocation', f'hdfs://{hadoop_host}:9000/ckpt/all') \
    .foreachBatch(writeBatchAll) \
    .start()

df = df.where('quantity>0') \
    .withColumn('mid_price', (df.best_ask + df.best_bid) / 2)
newOrders = df.withColumn('q_spread', (df.best_ask - df.best_bid) / df.mid_price)


# df = df \
#     .groupBy(F.window('time', '1 minute'), 'basequote', 'exchange', 'side') \
#     .agg((F.sum(df.price * df.quantity) / F.sum(df.quantity)).alias('price')) \
#     .selectExpr('window.start AS start', 'basequote', 'exchange', 'price', 'side')
#
# query3 = df.writeStream.format("console").start()


# one_hour = 3600000

# hour_window = df.groupBy('basequote', 'exchange', 'price', 'bid_or_ask').avg()
# df = df.withColumn("epoch_seconds", df.time.cast("long"))

# new_orders = new_orders\
#     .withColumn('avg_size', F.avg('quantity').over(hour_window)) \
#     .withColumn('stddev_size', F.stddev('quantity').over(hour_window))
def whale_score(p, q, q_spread, side, mid_price, active_bids, active_asks, avg, var):
    tmp = avg ** 2
    geo_mu = tmp / math.sqrt(tmp + var)
    # try:
    geo_sigma = math.exp(math.sqrt(math.log(1 + var / tmp)))
    # except:
    #     with open('./logs/bugs', 'wr+') as f:
    #         print(p, q, q_spread, side, mid_price, active_bids, active_asks, avg, var)
    #         print(tmp)
    #     print(var, tmp, avg)
    #     geo_sigma = 100
    q_score = geo_mu * (geo_sigma ** 2)
    if q < q_score:
        return 0
    w = 1
    for i in range(2):
        q_score *= geo_sigma
        if q < q_score:
            break
        w += 1
    if q_spread > 0.01:
        w += 1
    if side * (mid_price - p) / mid_price < 0.001:
        w += 1
    if q > (active_bids - active_asks) * side * 10:
        w += 1
    return w


spark.udf.register("ws_udf", whale_score, IntegerType())
col_expr = expr("ws_udf(price, quantity, q_spread, side, mid_price, active_bids, active_asks, avg, var)")
newOrders = newOrders \
    .withColumn('whale_score', col_expr)

newOrdersQuery = newOrders \
    .writeStream \
    .option('checkpointLocation', f'hdfs://{hadoop_host}:9000/ckpt/newOrders') \
    .foreachBatch(writeBatchNew) \
    .start()
allQuery.awaitTermination()
newOrdersQuery.awaitTermination()
# query3.awaitTermination()
