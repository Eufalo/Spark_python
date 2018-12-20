from engine import RecommendationEngine
from pyspark import SparkContext, SparkConf
import os
def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])

    return sc
sc = init_spark_context()
dataset_path = os.path.join(".",'anime')
recommendation_engine = RecommendationEngine(sc, dataset_path)
ratings = recommendation_engine.get_top_ratings_category (1, 4,"Mystery")
#ratings = recommendation_engine.get_top_ratings(1,4)
#calgoris=recommendation_engine.get_top_ratings(1,10)
#recommendation_engine.get_all_tipes()
#recommendation_engine.get_top_ratings_tips(13,10,"TV")
print(ratings)
#print(calgoris)
