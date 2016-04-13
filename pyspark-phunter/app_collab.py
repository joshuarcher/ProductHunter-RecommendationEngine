from __future__ import print_function

import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType

conf = SparkConf().setAppName("phunter_spark")
spContext = SparkContext(conf=conf)
sqlContext = SQLContext(spContext)

USER_ID = 0
# Rank: 10
# Regul: 0.1
# Iter: 20
# Dist: 0.168762733225

SQL_IP = sys.argv[1]
SQL_DB_NAME = sys.argv[2]
SQL_USER = sys.argv[3]
SQL_PSWD  = sys.argv[4]

BEST_RANK = int(10)
BEST_ITERATION = int(20)
BEST_REGULATION = float(0.1)

TABLE_PRODUCTS  = "Product"
TABLE_VOTES = "Vote"
TABLE_RECOMMENDATIONS = "Recommendation"

# Read data from cloudSQL
# create dataFrams
jdbcDriver = 'com.mysql.jdbc.Driver'
jdbcUrl = 'jdbc:mysql://{}:3306/{}?user={}&password={}'.format(
                        SQL_IP, SQL_DB_NAME, SQL_USER, SQL_PSWD)

dframeAccos = sqlContext.load(source='jdbc', driver=jdbcDriver, url=jdbcUrl, dbtable=TABLE_PRODUCTS)
dframeRates = sqlContext.load(source='jdbc', driver=jdbcDriver, url=jdbcUrl, dbtable=TABLE_VOTES)

# Returns only the accommodations that have not been rated by our user
rddPotential  = dframeAccos.rdd.filter(lambda x: x[0] not in dframeUserRatings)
pairsPotential = rddPotential.map(lambda x: (USER_ID, x[0]))

# split sets
rddTrain, rddValidate, rddTest = dframeRates.rdd.randomSplit([7,2,1])

# build model with best values from find_model.py
model = ALS.train(rddTrain, BEST_RANK, BEST_ITERATION, BEST_REGULATION)

# calculate predictions
predictions = model.predictAll(pairsPotential).map(lambda p: (int(p[0]), int(p[1]), float(p[2])))

# get top 5
topPredictions = predictions.takeOrdered(5, key=lambda x: -x[2])
print("top predictions: {}".format(topPredictions))

schema = StructType([StructField("userId", StringType(), True), StructField("prodId", StringType(), True), StructField("prediction", FloatType(), True)])


# save the top predictions
dframesToSave = sqlContext.createDataFrame(topPredictions, schema)
dframesToSave.write.jdbc(url=jdbcUrl, table=TABLE_RECOMMENDATIONS, mode='overwrite')


