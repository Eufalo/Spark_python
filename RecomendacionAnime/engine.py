import os
import shlex
from pyspark.mllib.recommendation import ALS

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (animeID, ratings_iterable)
    returns (animeID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


class RecommendationEngine:
    """A anime recommendation engine
    """

    def __count_and_average_ratings(self):
        """Updates the animes ratings counts from
        the current data self.ratings_RDD
        """
        logger.info("Counting anime ratings...")
        anime_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        anime_ID_with_avg_ratings_RDD = anime_ID_with_ratings_RDD.map(get_counts_and_averages)
        self.animes_rating_counts_RDD = anime_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))


    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.model = ALS.train(self.ratings_RDD, self.rank,seed=self.seed, iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("ALS model built!")



    def __predict_ratings(self, user_and_anime_RDD):
        """Gets predictions for a given (userID, animeID) formatted RDD
        Returns: an RDD with format (animeTitle, animeRating, numRatings)
        """

        predicted_RDD = self.model.predictAll(user_and_anime_RDD)

        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))

        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.animes_titles_RDD).join(self.animes_rating_counts_RDD)

        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        return predicted_rating_title_and_count_RDD

    def get_average_rating_for_animes_id(self, anime_id):
        "Given the anime_id you return the avereged rating for this anime"
        anime_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        anime_ID_with_avg_ratings_RDD = anime_ID_with_ratings_RDD.map(get_counts_and_averages)
        return anime_ID_with_avg_ratings_RDD.filter(lambda anime: anime[0] == anime_id).collect()

    def get_rated_animes(self, user_id):
        "Given a user_id Return all user rates for the animes"
        rated_animes = self.ratings_RDD.filter(lambda rating: rating[0] == user_id).map(lambda x: (x[1],x[2])).join(self.animes_titles_RDD).collect()
        return rated_animes

    def add_ratings(self, ratings):
        """Add additional anime ratings in the format (user_id, anime_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings_RDD = self.sc.parallelize(ratings)
        # Add new ratings to the existing ones
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # Re-compute anime ratings count
        self.__count_and_average_ratings()
        # Re-train the ALS model with the new ratings
        self.__train_model()

        return ratings

    def get_ratings_for_anime_ids(self, user_id, anime_ids):
        """Given a user_id and a list of anime_ids, predict ratings for them
        """
        requested_animes_RDD = self.sc.parallelize(anime_ids).map(lambda x: (user_id, x))
        # Get predicted ratings

        ratings = self.__predict_ratings(requested_animes_RDD).collect()

        return ratings

    def get_top_ratings(self, user_id, animes_count):
        """Recommends up to animes_count top unrated animes to user_id
        """
        # Get pairs of (userID, animeID) for user_id unrated animes
        user_unrated_animes_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id)\
                                                 .map(lambda x: (user_id, x[1])).distinct()
        print(user_unrated_animes_RDD.collect())
        # Get predicted ratings
        ratings = self.__predict_ratings(user_unrated_animes_RDD).filter(lambda r: r[2]>=25).takeOrdered(animes_count, key=lambda x: -x[1])

        return ratings

    def get_all_categorys(self):
        """Return all the category for the animes_RDD"""
        category_RDD=self.animes_RDD.map(lambda x:(str(x[-2]).split("|"))).flatMap(lambda x: [(item,1) for item in x]).groupByKey().map(lambda x: (x[0]))
        return category_RDD.collect()

    def get_all_tipes(self):
        """Return all the tipes for the animes_RDD

        tipes_RDD=self.animes_RDD.map(lambda x:(str(x[-1]),1)).groupByKey().map(lambda x: (x[0]))
        print(tipes_RDD.collect())
        return tipes_RDD.collect()
        """
        tips=('TV', 'Movie', 'Special', 'ONA','Unknown','Music');
        return tips
    def get_top_ratings_tips(self, user_id, animes_count,tipe):
        """Given the category recommends up to animes_count top for animes.category to user_id
        """
        # Get pairs of (userID, animeID) for user_id unrated animes
        user_unrated_animes_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id)\
                                                 .map(lambda x: (x[1],user_id)).distinct() #we need the tuple id_anime user_id to join the RDD with animes_RDD

        # Get RDD with the id_anime, anime title rating category and platform
        idanime_title_ratig_category_platformRDD = user_unrated_animes_RDD.union(self.animes_RDD)
        #Get RDD pairs of( animeID,tipe)
        idanime_tipe=idanime_title_ratig_category_platformRDD.map(lambda x: (x[0],x[-1])).filter(lambda x: x[1]==tipe)
        # Get pairs of (userID, animeID)
        user_animes_RDD=idanime_tipe.map(lambda x: (user_id,x[0]))
        #print(user_animes_RDD.collect())
        ratings = self.__predict_ratings(user_animes_RDD).filter(lambda r: r[2]>=25).takeOrdered(animes_count, key=lambda x: -x[1])
        return rating 

    def get_top_ratings_category(self, user_id, animes_count,category):
        """Given the category recommends up to animes_count top for animes.category to user_id
        """
        # Get pairs of (userID, animeID) for user_id unrated animes
        user_unrated_animes_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id)\
                                                 .map(lambda x: (x[1],user_id)).distinct() #we need the tuple id_anime user_id to join the RDD with animes_RDD

        # Get RDD with the id_anime, anime title rating category and platform
        idanime_title_ratig_category_platformRDD = user_unrated_animes_RDD.union(self.animes_RDD)
        #Get RDD pairs of( animeID,list(category))
        # With Flatmap -> Given RDD pairs of( animeID,list(category)) return a rdd paris (animeID,category)->Repited animeID like len(list(category))
        title_category_RDD=idanime_title_ratig_category_platformRDD.map(lambda x:(x[0],str(x[-2]).split("|"))).flatMap(lambda x: [(x[0],item) for item in x[1]]).filter(lambda x: x[1]==category)
        # Get pairs of (userID, animeID) for user_id unrated and category=category animes
        user_unrated_category_anime=title_category_RDD.map(lambda x: (user_id,x[0]))

        ratings = self.__predict_ratings(user_unrated_category_anime).filter(lambda r: r[2]>=25).takeOrdered(animes_count, key=lambda x: -x[1])

        return ratings


    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'Anime_ratings.csv')
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        #userID;animeID;rating;date
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_raw_data_header)\
            .map(lambda line: line.split(";")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
        # Load animes data for later use
        logger.info("Loading animes data...")
        animes_file_path = os.path.join(dataset_path, 'Animelist.csv')
        animes_raw_RDD = self.sc.textFile(animes_file_path)
        animes_raw_data_header = animes_raw_RDD.take(1)[0]
        #id;titulo;titulo_ingles;valoracion;generos;tipo
        self.animes_RDD = animes_raw_RDD.filter(lambda line: line!=animes_raw_data_header)\
            .map(lambda line: line.split(";")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2],tokens[3],tokens[4],tokens[5])).cache()
        self.animes_titles_RDD = self.animes_RDD.map(lambda x: (int(x[0]),x[1])).cache()

        # Pre-calculate animes ratings counts
        self.__count_and_average_ratings()

        # Train the model
        self.rank = 8
        self.seed = 5
        self.iterations = 10
        self.regularization_parameter = 0.1
        self.__train_model()
