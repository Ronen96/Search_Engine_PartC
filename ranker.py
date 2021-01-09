# you can change whatever you want in this module, just make sure it doesn't 
# break the searcher module
import math

from numpy import dot
from numpy.dual import norm


class Ranker:
    def __init__(self):
        pass

    @staticmethod
    def rank_relevant_docs(relevant_docs, norm_query, k=None):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param norm_query:
        :param k: number of most relevant docs to return, default to everything.
        :param relevant_docs: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        ranked_results = {}
        for doc in relevant_docs.keys():
            sum_wij_wiq = dot(relevant_docs[doc], norm_query)
            # sum_wij2 = sum([x ** 2 for x in relevant_docs[doc]])
            # sum_wiq2 = sum([x ** 2 for x in norm_query])
            # cos_similarity = sum_wij_wiq / math.sqrt(sum_wij2 * sum_wiq2)
            cos_similarity = sum_wij_wiq / (norm(relevant_docs[doc]) * norm(norm_query))
            # if cos_similarity >= 0.5:
            ranked_results[doc] = cos_similarity

        ranked_results = sorted(ranked_results.items(), key=lambda item: item[1], reverse=True)

        if k is not None:
            ranked_results = ranked_results[:k]
        return [d[0] for d in ranked_results]

