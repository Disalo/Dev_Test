import random

# MOVIES = ['Titanic',
#           'Docker: Unleashed',
#           'Julia: Elegant and Hip',
#           'Python : The Slow Snake',
#           'C: Kids these days dont know how to compile code',
#           'Shawshank Redemption']

class RecommenderClass:

    """Class for grouping all my functions"""

    def __init__(self, list_of_movies):
        self.list_of_movies = list_of_movies

    def recommend_movie(self):
        """Randomly recommend one movie from a list."""
        result = random.choice(self.list_of_movies)
        return result