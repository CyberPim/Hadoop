from mrjob.step import MRStep
from mrjob.job import MRJob

class MovieRatings(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies, combiner=self.combine_ratings, reducer=self.sum_rating),
            MRStep(reducer=self.sort_movies)
        ]

    # Get the movies in u.data and return the id
    def mapper_get_movies(self, _, line):
        (_, movieID, _, _) = line.split('\t')
        yield movieID, 1
    
    # Combine the ratings
    def combine_ratings(self, movie_id, ratings):
        # return the result
        yield movie_id, sum(ratings)
    
    # Calculate the sum of ratings
    def sum_rating(self, movie_id, ratings):
        # Return the sum of ratings for eacht movie id
        yield None, (sum(ratings), movie_id)

    # Sort the movies    
    def sort_movies(self, _, movies):
        for count, movie_id in sorted(movies):
            # Return the sorted list
            yield (int(movie_id), int(count))

# Init movieratings and run the code
if __name__ == "__main__":
    MovieRatings.run()