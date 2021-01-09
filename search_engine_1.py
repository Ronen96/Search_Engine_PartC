import time

import glob2
import pandas as pd
from nltk.corpus import wordnet

import metrics
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
from spellchecker import SpellChecker


# DO NOT CHANGE THE CLASS NAME
class SearchEngine:

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation, but you must have a parser and an indexer.
    def __init__(self, config=None):
        self._config = config
        self._parser = Parse()
        self._indexer = Indexer(config)
        self._model = None

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def build_index_from_parquet(self, fn):
        """
        Reads parquet file and passes it to the parser, then indexer.
        Input:
            fn - path to parquet file
        Output:
            No output, just modifies the internal _indexer object.
        """
        r = ReadFile()
        df = r.read_file(fn)
        documents_list = df
        # Iterate over every document in the file
        number_of_documents = 0
        for idx, document in enumerate(documents_list):
            # parse the document
            parsed_document = self._parser.parse_doc(document)
            number_of_documents += 1
            # index the document data
            self._indexer.add_new_doc(parsed_document)
        # self._indexer.save_index('indverted_idx.pkl')
        self._indexer.save_index('idx_bench.pkl')
        print('Finished parsing and indexing.', 'inverted_index_len:', len(self._indexer.inverted_idx.keys()))

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_index(self, fn):
        """
        Loads a pre-computed index (or indices) so we can answer queries.
        Input:
            fn - file name of pickled index.
        """
        self._indexer.load_index(fn)

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def load_precomputed_model(self, model_dir=None):
        """
        Loads a pre-computed model (or models) so we can answer queries.
        This is where you would load models like word2vec, LSI, LDA, etc. and
        assign to self._model, which is passed on to the searcher at query time.
        """
        pass

    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation as you see fit.
    def search(self, query):
        """
        Executes a query over an existing index and returns the number of
        relevant docs and an ordered list of search results.
        Input:
            query - string.
        Output:
            A tuple containing the number of relevant search results, and
            a list of tweet_ids where the first element is the most relavant
            and the last is the least relevant result.
        """
        searcher = Searcher(self._parser, self._indexer, model=self._model)
        query_as_list = self._parser.parse_sentence(query)
        inverted_idx = self.indexer.inverted_idx
        spell = SpellChecker()
        misspelled = spell.unknown(query_as_list)
        assist = [x.lower() for x in query_as_list]  # all the query terms in lower case

        for word in misspelled:
            if word.upper() in inverted_idx.keys() or word.lower() in inverted_idx.keys() or ' ' in word:
                continue  # if the word is in the inverted index- no correction need

            word_idx = assist.index(word)
            corrections = spell.edit_distance_1(word)  # list of all the suggested corrections with distance value 1
            corrections_dict = {}
            # check if the suggested corrections is in inverted index and collect the frequency of each correction
            for correction in corrections:
                if correction.upper() in inverted_idx.keys():
                    corrections_dict[correction] = inverted_idx[correction.upper()]

                if correction.lower() in inverted_idx.keys():
                    corrections_dict[correction] = inverted_idx[correction.lower()]

            if corrections_dict:
                query_as_list[word_idx] = max(corrections_dict,
                                              key=corrections_dict.get)  # choose the most common correction
            else:
                query_as_list[word_idx] = spell.correction(word)

        new_query = ' '.join(query_as_list)
        relevant_docs = searcher.search(new_query)

        return relevant_docs

    @property
    def indexer(self):
        return self._indexer

# def main():
#     config = ConfigClass()
#     path = config.get__corpusPath()
#     search_engine = SearchEngine(config)
#
#     files_in_folder = glob2.glob(path + '/**/*.parquet')
#     start_time = time.time()
#     for fp in files_in_folder:
#         search_engine.build_index_from_parquet(fp)
#
#     end_time = time.time()
#     print("--- %s seconds ---" % (end_time - start_time))
#
#     search_engine.indexer.load_index('idx_bench.pkl')

