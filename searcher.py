import math

from ranker import Ranker
import utils


# DO NOT MODIFY CLASS NAME
class Searcher:
    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit. The model 
    # parameter allows you to pass in a precomputed model that is already in 
    # memory for the searcher to use such as LSI, LDA, Word2vec models. 
    # MAKE SURE YOU DON'T LOAD A MODEL INTO MEMORY HERE AS THIS IS RUN AT QUERY TIME.
    def __init__(self, parser, indexer, model=None):
        self._parser = parser
        self._indexer = indexer
        self._ranker = Ranker()
        self._model = model

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def search(self, query, k=1000):
        """ 
        Executes a query over an existing index and returns the number of 
        relevant docs and an ordered list of search results (tweet ids).
        Input:
            query - string.
            k - number of top results to return, default to everything.
        Output:
            A tuple containing the number of relevant search results, and 
            a list of tweet_ids where the first element is the most relavant 
            and the last is the least relevant result.
        """
        query_as_list = self._parser.parse_sentence(query)

        relevant_docs, query_vec = self._relevant_docs_from_posting(query_as_list)
        n_relevant = len(relevant_docs)

        ranked_doc_ids = Ranker.rank_relevant_docs(relevant_docs, query_vec)
        # if n_relevant > k:
        #     proportion = round(n_relevant * 0.75)
        #     ranked_doc_ids = ranked_doc_ids[:proportion]
        return n_relevant, ranked_doc_ids

    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def _relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query_as_list: parsed query tokens
        :return: dictionary of relevant documents mapping doc_id to document frequency.
        """
        relevant_docs = {}  # key- tweet id , value- words vector
        docs_vector = {}  # key- docID, value- vector of words

        query_dict = {}  # to check how many times word appear in the query
        for term in query_as_list:
            try:
                if term in self._indexer.inverted_idx.keys():
                    temp_term = term
                elif term.lower() in self._indexer.inverted_idx.keys():
                    temp_term = term.lower()
                elif term.upper() in self._indexer.inverted_idx.keys():
                    temp_term = term.upper()
                else:
                    continue

                if temp_term not in query_dict.keys():
                    query_dict[temp_term] = 1
                else:
                    query_dict[temp_term] += 1

            except:
                print('term {} not found in posting'.format(term))

        i = 0
        for term in query_dict.keys():
            for doc_id, fi, doc_len in self._indexer.postingDict[term]:
                if doc_id not in docs_vector.keys():
                    docs_vector[doc_id] = [0] * len(query_dict.keys())
                tf = fi / doc_len #self._indexer.docs_dict[doc_id][0]
                idf = math.log(len(self._indexer.docs_dict.keys()) / self._indexer.inverted_idx[term], 2)
                docs_vector[doc_id][i] = tf * idf
            i += 1

        norm_query = {}
        for key in query_dict:
            norm_query[key] = query_dict[key] / len(query_dict.keys())
        return docs_vector, list(norm_query.values())

