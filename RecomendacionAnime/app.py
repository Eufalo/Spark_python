from flask import Blueprint
main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, render_template, request, session, redirect, url_for, Response, send_file
'''
&&& APP &&&
/<int:user_id>/category/<int:anime_count>/<string:category> -> Give the user_id and category return the top(anime_count) rating for this anime category
/infocategory -> #Return all the cateogrys
/info/<int:anime_id> -> #Give the anime_id return the average rating for this anime
/<int:user_id>/ratings/top/<int:count> -> #Given the user and numerber of anime you want to see Return the top unrated anime for this user
/<int:user_id>/rated -> #Given the user_id gives the history rating
/<int:user_id>/tipe/<int:anime_count>/<string:tipe> -> #Give the user_id and tipe return the top(anime_count) rating for this anime tipe
/infotipe'-> #Return all the tipes or the format you can find this anime
'''

@main.route('/',methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        animeName =  request.form.get('animeName')
        res = recommendation_engine.queryanime(animeName)
        return render_template('result.html', animename = animeName, search_res = res)
    return render_template('search.html')

@main.route('/<int:user_id>/category/<int:anime_count>/<string:category>')
# Give the user_id and category return the top(anime_count) rating for this anime category
def animecategory(user_id,category,anime_count):
    print(category)
    rating = recommendation_engine.get_top_ratings_category(user_id, anime_count,category)
    return json.dumps(rating)

@main.route('/infocategory')
#Return all the cateogrys
def anime_category_Info():
    rating = recommendation_engine.get_all_categorys()
    return json.dumps(rating)

@main.route('/<int:user_id>/tipe/<int:anime_count>/<string:tipe>')
# Give the user_id and tipe return the top(anime_count) rating for this anime tipe
def animetipe(user_id,tipe,anime_count):
    rating = recommendation_engine.get_top_ratings_tips(user_id, anime_count,tipe)
    return json.dumps(rating)

@main.route('/infotipe')
#Return all the tipes or the format you can find this anime
def anime_tipe_Info():
    rating = recommendation_engine.get_all_tipes()
    return json.dumps(rating)

@main.route('/info/<int:anime_id>')
#Give the anime_id return the average rating for this anime
def animeInfo(anime_id):
    rating = recommendation_engine.get_average_rating_for_animes_id(anime_id)
    return json.dumps(rating)
    #return render_template('details.html',score = round(rating[0][1][1],2),num = rating[0][1][0], anime_id = anime_id)

@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
#Given the user and numerber of anime you want to see Return the top unrated anime for this user
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    #return render_template('recommendation.html', animes = top_ratings)
    return json.dumps(top_ratings)

@main.route("/<int:user_id>/ratings/<int:anime_id>", methods=["GET"])
def anime_ratings(user_id, anime_id):
    """Given a user_id and a list of anime_ids, predict ratings for them
    """
    logger.debug("User %s rating requested for anime %s", user_id, anime_id)
    ratings = recommendation_engine.get_ratings_for_anime_ids(user_id, [anime_id])
    return json.dumps(ratings)
    #return render_template('details.html',score = rating, anime_id = anime_id)

@main.route("/<int:user_id>/rating/<int:anime_id>", methods = ["POST"])
def add_rating(user_id,anime_id):
    rating = [(user_id, (int)(anime_id), (int)(request.form.get('score')))]
    recommendation_engine.add_ratings(rating)
    return redirect("/0/rated")

@main.route("/<int:user_id>/rated")
def rated(user_id):
    "Given the user_id gives the history rating"
    rated_animes = recommendation_engine.get_rated_animes(user_id)
    #return render_template('rated.html', rated = rated_animes)
    return json.dumps(rated_animes)

@main.route("/<int:user_id>/ratings", methods = ["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    # create a list with the format required by the negine (user_id, anime_id, rating)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # add them to the model using then engine API
    recommendation_engine.add_ratings(ratings)

    return json.dumps(ratings)


def create_app(spark_context, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)

    app = Flask(__name__, static_url_path='/static')
    app.register_blueprint(main)
    return app
