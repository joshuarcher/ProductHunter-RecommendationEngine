import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

SQL_IP = sys.argv[1]
SQL_DB_NAME = sys.argv[2]
SQL_USER = sys.argv[3]
SQL_PSWD = sys.argv[4]

conf = SparkConf().setAppName("phunter_collaborative")
spcontext = SparkContext(conf=conf)
sqlContext = SQLContext(spcontext)

jbdcDriver = 'com.mysql.jdbc.Driver'
jdbcUrl = 'jdbc:mysql://{}:3306/{}?user={}&password={}'.format(
                        SQL_IP, SQL_DB_NAME, SQL_USER, SQL_PSWD)

# howFar() ran down below
def howFar(model, against, sizeAgainst):
  # don't use ratings
  againstNoRatings = against.map( lambda x: (int(x[1]), int(x[2])) )

  # use 1 as the rating to compare
  againstWiRatings = against.map( lambda x: ((int(x[1]), int(x[2])), int(1)) )

  # make prediction and map for later comparison
  predictions = model.predictAll(againstNoRatings).map( lambda p: ( (p[0],p[1]), p[2]) )

  # returns the pairs (prediction, rating)
  predictionsAndRatings = predictions.join(againstWiRatings).values()

  # return the variance
  return sqrt(predictionsAndRatings.map(lambda s: (s[0] - s[1]) ** 2).reduce(add) / float(sizeAgainst))
# end of howFar()

'''
Reading data from Cloud SQL where stored
Creating dataframes
'''
# dfRates = sqlContext.read.format('jdbc').options(url=jdbcUrl, dbtable='Rating').load()
dfRates = sqlContext.read.format('jdbc').options(url=jdbcUrl, dbtable='Vote').load()

print("dfRates: {}".format(dfRates))

sys.exit()
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

# try these compinations
ranks = [5,10,15]
reguls = [0.1,1]
iters = [5,10,20]

finalModel = None
finalRank = 0
finalRegul = float(0)
finalIter = -1
finalDist = float(1000)

# start training model on combinations of inputs
for cRank, cRegul, cIter in itertools.product(ranks, reguls, iters):
  print("rddTrain: {}\n cRank: {}\n cIter: {}\n cRegul: {}".format(rddTrain, cRank, cIter, cRegul))
  model = ALS.train(rddTrain, cRank, cIter, float(cRegul))
  dist = howFar(model, rddValidate, nbValidate)
  if dist < finalDist:
    print("Best dist so far: {}".format(dist))
    finalModel = model
    finalRank = cRank
    finalRegul = cRegul
    finalIter = cIter
    finalDist = dist
# end training model

print("Rank: {}".format(finalRank))
print("Regul: {}".format(finalRegul))
print("Iter: {}".format(finalIter))
print("Dist: {}".format(finalDist))





'''
This script was adapted from Google's spark-recommendation-engine tutorial
https://github.com/GoogleCloudPlatform/spark-recommendation-engine
'''