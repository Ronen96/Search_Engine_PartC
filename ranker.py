# you can change whatever you want in this module, just make sure it doesn't 
# break the searcher module
import math


class Ranker:
    def __init__(self):
        pass

    @staticmethod
    def rank_relevant_docs(relevant_docs, k=None):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param k: number of most relevant docs to return, default to everything.
        :param relevant_docs: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        ranked_results = {}
        for doc in relevant_docs.keys():
            sum_wij = sum(relevant_docs[doc])
            sum_wij2 = sum([x ** 2 for x in relevant_docs[doc]])
            sum_wiq2 = len(relevant_docs[doc])
            cos_similarity = sum_wij / math.sqrt(sum_wij2 * sum_wiq2)
            ranked_results[doc] = cos_similarity

        ranked_results = sorted(ranked_results.items(), key=lambda item: item[1], reverse=True)

        if k is not None:
            ranked_results = ranked_results[:k]
        return [d[0] for d in ranked_results]

