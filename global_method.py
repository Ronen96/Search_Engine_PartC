import collections
import copy
import itertools
import json
import operator
import pickle
import time
from configuration import ConfigClass
import utils
import statistics
from tqdm import tqdm


class GlobalMethod(object):
    associations_matrix = {}
    relevant_terms = []

    @classmethod
    def build_matrix(cls):

        start_time = time.time()
        print('load inverted index')

        with open('1K_terms.pkl', 'rb') as inverted_idx:
            inverted_idx = pickle.load(inverted_idx)
        print('inverted index loaded')
        inverted_idx = dict(inverted_idx)

        with open('posting_dict.pkl', 'rb') as posting_dict:
            posting_dict = pickle.load(posting_dict)
        posting_dict = dict(posting_dict)

        relevant_words = sorted(inverted_idx, key=inverted_idx.get, reverse=True)

        cls.associations_matrix = {i: [0] * len(relevant_words) for i in relevant_words}
        print('association matrix build with zeros')

        docs_set = set()
        docs_sets_dict = {}

        for word in tqdm(relevant_words):

            for doc_id, freq, doc_len in posting_dict[word]:
                docs_set.add(doc_id)

            docs_sets_dict[word] = docs_set

            for key_word in tqdm(docs_sets_dict.keys()):
                if key_word == word or key_word == word.upper() or key_word == word.lower():
                    cii = 0
                    for val in posting_dict[key_word]:
                        cii += val[1] ** 2

                    idx = relevant_words.index(word)
                    cls.associations_matrix[word][idx] = cii

                else:
                    cij = 0
                    common_docs = docs_sets_dict[word].intersection(docs_sets_dict[key_word])
                    for doc in common_docs:
                        temp_cij = 0
                        for val in posting_dict[word]:
                            if val[0] == doc:
                                temp_cij = val[1]
                        for val2 in posting_dict[key_word]:
                            if val2[0] == doc:
                                temp_cij = temp_cij * val2[1]
                        cij += temp_cij

                    idx_i = relevant_words.index(word)
                    idx_j = relevant_words.index(key_word)
                    cls.associations_matrix[key_word][idx_i] = cij
                    cls.associations_matrix[word][idx_j] = cij

        print('association matrix build without normalize')

        for row in tqdm(cls.associations_matrix.keys()):
            for idx_col in range(len(cls.associations_matrix[row])):
                idx_row = relevant_words.index(row)
                col = relevant_words[idx_col]

                cii = cls.associations_matrix[row][idx_row]
                cjj = cls.associations_matrix[col][idx_col]
                cij = cls.associations_matrix[row][idx_col]
                if row != col:
                    demon = (cii + cjj - cij)
                    sij = cij / demon
                    cls.associations_matrix[row][idx_col] = sij

        print('association matrix build with normalize')

        #
        utils.save_obj(cls.associations_matrix, "associations_matrix")

        end_time = time.time()
        print("--- %s seconds ---" % (end_time - start_time))

        return cls.associations_matrix

    @classmethod
    def expand_query(cls, query):

        with open('associations_matrix.pkl', 'rb') as matrix_from_file:
            association_matrix = pickle.load(matrix_from_file)
        expansion = []
        relevant_terms = sorted(association_matrix, key=association_matrix.get, reverse=True)
        for term in query:
            if term not in association_matrix.keys():
                continue
            lst = association_matrix[term]
            idx_max_val = lst.index(max(lst))
            expansion.append(relevant_terms[idx_max_val])

        return query.extend(expansion)

# GlobalMethod.build_matrix()
