import webapp2
import json

class RecommendationEngineHandler(webapp2.RequestHandler):
  def get(self):
    pass

class LikeHandler(webapp2.RequestHandler):
  def post(self):
    pass

api = webapp2.WSGIApplication([
    ('/api/get_recommendations', RecommendationEngineHandler)
   ,('/api/user_like', LikeHandler)
  ]
)