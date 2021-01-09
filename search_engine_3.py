import pickle
import time

import glob2
import pandas as pd
from nltk.corpus import wordnet

import metrics
from advanced_parser import Advanced_Parse
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
from spellchecker import SpellChecker


# DO NOT CHANGE THE CLASS NAME
class SearchEngine:

    # ---------------------------------------advanced parser------------------------------------------------------
    # DO NOT MODIFY THIS SIGNATURE
    # You can change the internal implementation, but you must have a parser and an indexer.
    def __init__(self, config=None):
        self._config = config
        self._parser = Parse()
        self._advanced_parser = Advanced_Parse()
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
        number_of_files = 0
        for idx, document in enumerate(documents_list):
            # parse the document
            parsed_document = self._advanced_parser.parse_doc(document)
            # index the document data
            self._indexer.add_new_doc(parsed_document)
            #     self._indexer.docs_dict = {}
            number_of_files += 1
        self._indexer.postingDict = {}
        self._indexer.docs_dict = {}

        # self._indexer.save_index('idx_bench_advanced_parser.pkl')
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

        relevant_docs = searcher.search(query)

        return relevant_docs

    @property
    def indexer(self):
        return self._indexer


def main():
    config = ConfigClass()
    path = config.get__corpusPath()
    search_engine = SearchEngine(config)

    files_in_folder = glob2.glob(path + '/**/*.parquet')
    start_time = time.time()
    for fp in files_in_folder:
        search_engine.build_index_from_parquet(fp)

    end_time = time.time()
    print("--- %s seconds ---" % (end_time - start_time))

    # ---------build inverted index---------------
    with open('inverted_idx.pkl', 'wb') as f:
        pickle.dump(search_engine.indexer.inverted_idx, f, pickle.HIGHEST_PROTOCOL)

    # with open('indverted_idx.pkl', 'rb') as f:
    #     inverted_index = pickle.load(f)
    #
    # with open('posting_dict.pkl', 'rb') as f:
    #     posting_dict = pickle.load(f)



    # files_in_folder = glob2.glob('C:\\Users\\barif\\PycharmProjects\\Search_Engine_PartC\\posting' + '/**/*.pkl')
    # with open('posting_dict_75906.pkl', 'rb') as f:
    #     posting1 = pickle.load(f)
    # for file in files_in_folder:
    #     with open(file, 'rb') as f:
    #         posting2 = pickle.load(f)
    #     new = merge_posting_files(posting1, posting2)
    #     posting1 = new
    # with open('posting_dict', 'wb') as f:
    #     pickle.dump(new, f)
    #
    # return posting1





    # with open('posting_dict.pkl', 'wb') as f:
    #     pickle.dump(search_engine.indexer.postingDict, f, pickle.HIGHEST_PROTOCOL)

    # with open('indverted_idx.pkl', 'rb') as f:
    #     search_engine.indexer.inverted_idx = pickle.load(f)
    #
    # with open('posting_dict.pkl', 'rb') as f:
    #     search_engine.indexer.postingDict = pickle.load(f)

    # sorted_inverted = sorted(search_engine.indexer.inverted_idx.items(), key=lambda item: item[1], reverse=True)
    # sorted_posting = sorted(search_engine.indexer.postingDict.items(), key=lambda item: item[1], reverse=True)
    #
    # with open('3K_terms.pkl', 'wb') as f:
    #     pickle.dump(sorted_inverted[:3000], f, pickle.HIGHEST_PROTOCOL)
    #
    # with open('posting_3k.pkl', 'wb') as f:
    #     pickle.dump(sorted_posting[:3000], f, pickle.HIGHEST_PROTOCOL)


#     search_engine.indexer.load_index('idx_bench_advanced_parser.pkl')

