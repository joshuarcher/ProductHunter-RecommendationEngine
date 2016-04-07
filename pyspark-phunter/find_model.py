import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.recommendation import ALS MatrixFactorizationModel, Rating

SQL_IP = sys.argv[1]
SQL_DB_NAME = sys.argv[1]
SQL_USER = sys.argv[3]
SQL_PSWD = sys.argv[4]

conf = SparkConf().setAppName("phunter_collaborative")
sp-context = SparkContext(conf=conf)
sql-context = SQLContext(sp-context)

jbdcDriver = 'com.mysql.jdbc.Driver'
jdbcUrl = 'jdbc:mysql://{}:3306/{}?user={}&password={}'.format(
                        SQL_IP, SQL_DB_NAME, SQL_USER, SQL_PSWD)

'''
Reading data from Cloud SQL where stored
Creating dataframes
'''
dfRates = sql-context.read.format('jdbc').options(url=jdbcUrl, dbtable='Like').load()

rddUserRatings = dfRates.filter(dfRates.userId == 0).rdd
print(rddUserRatings.count())

'''
We need to split the data into: training, validation, testing
lets go for 70-20-10
'''
rddRates = dfRates.rdd
rddTrain, rddValidate, rddTest = rddRates.randomSplit([7,2,1])

'''
add user ratings to training model
'''
rddTrain.union(rddUserRatings)
nbValidate = rddValidate.count()
nbTest = rddTest.count()

print('Training: {}, validation: {}, test: {}'.format(rddTrain.count()
                                                     ,rddValidate.count()
                                                     ,rddTest.count()
                                                     ))





'''
This script was adapted from Google's spark-recommendation-engine tutorial
https://github.com/GoogleCloudPlatform/spark-recommendation-engine
'''