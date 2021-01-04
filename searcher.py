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
    def search(self, query, k=None):
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

        relevant_docs = self._relevant_docs_from_posting(query_as_list)
        n_relevant = len(relevant_docs)
        ranked_doc_ids = Ranker.rank_relevant_docs(relevant_docs)
        return n_relevant, ranked_doc_ids

    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def _relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query_as_list: parsed query tokens
        :return: dictionary of relevant documents mapping doc_id to document frequency.
        """
        relevant_docs = {}  # key- tweet id , value
        terms = {}  # key- term , value- (tweet id, fi)
        docs_content = {}

        query_dict = {}  # to check how many times word appear in the query
        for term in query_as_list:
            try:
                if term in self.inverted_index.keys():
                    temp_term = term
                elif term.lower() in self.inverted_index.keys():
                    temp_term = term.lower()
                elif term.upper() in self.inverted_index.keys():
                    temp_term = term.upper()
                else:
                    continue
                dict_of_doc_ids = self.extract_from_posting_file(temp_term, self.inverted_index[temp_term][1])
                key = [*dict_of_doc_ids][0]
                terms[temp_term] = dict_of_doc_ids[key][0]
                for doc in terms[temp_term]:
                    if doc[0] not in docs_content.keys():
                        docs_content[doc[0]] = [[temp_term, doc[1]]]
                        relevant_docs[doc[0]] = []
                    else:
                        exists = False
                        for pair in docs_content[doc[0]]:
                            if pair[0] == term:
                                pair[1] += 1
                                exists = True
                        if not exists:
                            docs_content[doc[0]].append([term, doc[1]])

                if temp_term not in query_dict.keys():
                    query_dict[temp_term] = 1
                else:
                    query_dict[temp_term] += 1

            except:
                print('term {} not found in posting'.format(term))

        idf = []
        # query_vec = []
        #
        # for term in query_dict:
        #     query_vec.append(query_dict[term])
        #
        # normalized_query = np.divide(query_vec, max(query_vec))
        for word in query_dict.keys():
            try:
                if word in self.inverted_index.keys():
                    dfi = self.inverted_index[word][0]
                    idf.append(math.log(num_of_docs_in_corpus / dfi, 2))

                    for doc in docs_content.keys():
                        exist = False
                        for pair in docs_content[doc]:
                            if word == pair[0]:
                                relevant_docs[doc].append(pair[1])
                                exist = True

                        if not exist:
                            relevant_docs[doc].append(0)

            except:
                print('term {} not found in inverted index'.format(word))

        # divide each element in the vector by thr max(f) of the doc. the information in docs_dict

        with open('docs_dict.json', 'r') as f:
            for line in f:
                j_content = json.loads(line)
                key = [*j_content][0]
                if key in relevant_docs.keys():
                    max_tf = j_content[key][0]
                    relevant_docs[key] = np.divide(relevant_docs[key], max_tf)
                    relevant_docs[key] = np.multiply(relevant_docs[key], idf)

        # calculate idf of each element in the vector (idf is log2(number of docs in the corpus \ df(from inverted index)

        # multiply tf*idf of each element

        # return relevant docs

        return relevant_docs  # , normalized_query
        # relevant_docs = {}
        # for term in query_as_list:
        #     posting_list = self._indexer.get_term_posting_list(term)
        #     for doc_id, tf, index_in_text in posting_list:
        #         df = relevant_docs.get(doc_id, 0)
        #         relevant_docs[doc_id] = df + 1
        # return relevant_docs
