import json
import pickle
import string
from string import ascii_lowercase

from nltk.corpus import stopwords


# DO NOT MODIFY CLASS NAME
class Indexer:
    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def __init__(self, config):
        self.config = config
        self.inverted_idx = {}
        self.postingDict = {}
        self.docs_dict = {}
        self.stop_words = stopwords.words('english')
        self.stop_words.extend(['https', 'http', 'rt', 'www', 't.co'])
        self.stop_words.extend(list(string.ascii_lowercase))
        self.num_of_docs_in_corpus = 0
        self.names_dict = {}

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = {}
        count_unique_words = 0
        doc_dictionary = document.term_doc_dictionary
        for term in doc_dictionary:
            if term.lower() not in self.stop_words:
                document_dictionary[term] = doc_dictionary[term]

        term_num_check = 1
        for term in document_dictionary.keys():
            try:  # Update inverted index and posting
                temp_term = ''

                if document_dictionary[term] == 1:  # save the amount of unique words in document
                    count_unique_words += 1
                # first char is number
                if term[0].isdigit():
                    to_index = False
                    if term.isdigit():
                        to_index = True
                    elif term[-1] == 'K' or term[-1] == 'M' or term[-1] == 'B':
                        to_index = True
                    elif '.' in term:
                        idx = term.find('.')
                        if term[idx + 1].isdigit():
                            to_index = True

                    if to_index:
                        temp_term = term.upper()
                        if temp_term not in self.inverted_idx.keys():
                            self.inverted_idx[temp_term] = 1
                            self.postingDict[temp_term] = []
                        else:
                            self.inverted_idx[temp_term] += 1

                elif '.' in term:
                    continue

                # first char is uppercase
                elif term[0].isupper():
                    # entity
                    if term in self.inverted_idx.keys():
                        temp_term = term
                        self.inverted_idx[term] += 1
                    elif term in self.names_dict:
                        temp_term = term
                        self.inverted_idx[temp_term] = 2
                        self.postingDict[temp_term] = [self.names_dict[temp_term]]
                        del (self.names_dict[temp_term])

                    elif ' ' in term:
                        self.names_dict[term] = (document.tweet_id, document_dictionary[term], sum(document_dictionary.values()))

                    # regular word
                    elif term.lower() in self.inverted_idx.keys():
                        temp_term = term.lower()
                        self.inverted_idx[temp_term] += 1
                    elif term.upper() in self.inverted_idx.keys():
                        temp_term = term.upper()
                        self.inverted_idx[temp_term] += 1

                    else:
                        temp_term = term.upper()
                        self.inverted_idx[temp_term] = 1
                        self.postingDict[temp_term] = []

                # other
                elif term[0].islower():
                    if term.lower() in self.inverted_idx.keys():
                        temp_term = term.lower()
                        self.inverted_idx[temp_term] += 1
                    elif term.upper() in self.inverted_idx.keys():
                        temp_term = term.lower()
                        self.inverted_idx[temp_term] = self.inverted_idx[term.upper()]
                        self.postingDict[temp_term] = self.postingDict[term.upper()]
                        self.inverted_idx[temp_term] += 1
                        del (self.inverted_idx[term.upper()])
                        del (self.postingDict[term.upper()])
                    else:
                        temp_term = term.lower()
                        self.inverted_idx[temp_term] = 1
                        self.postingDict[temp_term] = []

                if temp_term in self.postingDict.keys():
                    self.postingDict[temp_term].append((document.tweet_id, document_dictionary[term],
                                                        sum(document_dictionary.values())))
                term_num_check += 1

            except:
                pass
        self.num_of_docs_in_corpus += 1

        if document_dictionary:  # if dict isn't empty
            self.docs_dict[document.tweet_id] = (
                document_dictionary[max(document_dictionary, key=document_dictionary.get)], document.tweet_date,
                count_unique_words)

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_index(self, fn):
        """
        Loads a pre-computed index (or indices) so we can answer queries.
        Input:
            fn - file name of pickled index.
        """
        with open(fn, 'rb') as f:
            self.inverted_idx, self.postingDict, self.docs_dict = pickle.load(f)
            return self.inverted_idx, self.postingDict, self.docs_dict

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def save_index(self, fn):
        """
        Saves a pre-computed index (or indices) so we can save our work.
        Input:
              fn - file name of pickled index.
        """
        all_dicts = [self.inverted_idx, self.postingDict, self.docs_dict]
        with open(fn, 'wb') as f:
            pickle.dump(all_dicts, f, pickle.HIGHEST_PROTOCOL)

    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def _is_term_exist(self, term):
        """
        Checks if a term exist in the dictionary.
        """
        return term in self.postingDict

    # feel free to change the signature and/or implementation of this function 
    # or drop altogether.
    def get_term_posting_list(self, term):
        """
        Return the posting list from the index for a term.
        """
        return self.postingDict[term] if self._is_term_exist(term) else []
