import csv
import os
import pickle
import time

import glob2
import pandas as pd
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import metrics


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
        relevant_docs = searcher.search(query)
        return relevant_docs

    @property
    def indexer(self):
        return self._indexer


def main():
    config = ConfigClass()
    path = config.get__corpusPath()
    search_engine = SearchEngine(config)
#
    files_in_folder = glob2.glob(path + '/**/*.parquet')
    start_time = time.time()
    for fp in files_in_folder:
        search_engine.build_index_from_parquet(fp)

    end_time = time.time()
    print("--- %s seconds ---" % (end_time - start_time))
#
#     # search_engine.indexer.load_index('indverted_idx.pkl')
    search_engine.indexer.load_index('idx_bench.pkl')
#
    # with open('indverted_idx.pkl', 'rb') as f:
    #     search_engine.indexer.inverted_idx = pickle.load(f)
    #
    # with open('posting_dict.pkl', 'rb') as f:
    #     search_engine.indexer.postingDict = pickle.load(f)
    #
    # sorted_inverted = sorted(search_engine.indexer.inverted_idx.items(), key=lambda item: item[1], reverse=True)
    # sorted_posting = sorted(search_engine.indexer.postingDict.items(), key=lambda item: item[1], reverse=True)
    #
    # with open('3K_terms.pkl', 'wb') as f:
    #     pickle.dump(sorted_inverted[:3000], f, pickle.HIGHEST_PROTOCOL)
    #
    # with open('posting_3k.pkl', 'wb') as f:
    #     pickle.dump(sorted_posting[:3000], f, pickle.HIGHEST_PROTOCOL)
#
#     # write ranked result to csv
#     with open('queries.txt', encoding="utf8") as f:
#         # line_number = 0
#         data_to_csv = pd.DataFrame(columns=['query number', 'tweet id', 'Rank'])
#         data_to_csv2 = pd.DataFrame(columns=['query number', 'precision', 'precision@5', 'precision@10', 'precision@50',
#                                              'recall', 'MAP'])
#         for line in f.readlines():
#             if line != '\n':
#                 query_number = int(line[0])
#                 line = line[1:]
#                 n_relevant, ranked_doc_ids = search_engine.search(line)
#                 for doc, rank in ranked_doc_ids:
#                     data_to_csv = data_to_csv.append(
#                         {'query number': query_number, 'tweet id': doc, 'Rank': rank},
#                         ignore_index=True)
#                 #
#                 # check result
#                 df = pd.read_csv(config.get__corpusPath() + '/benchmark_lbls_train.csv')
#                 precision = metrics.precision(df, True, query_number)
#                 precision_5 = metrics.precision_at_n(df, query_number, 5)
#                 precision_10 = metrics.precision_at_n(df, query_number, 10)
#                 precision_50 = metrics.precision_at_n(df, query_number, 50)
#                 recall = metrics.recall(df, {query_number: n_relevant})
#                 MAP = metrics.map(df)
#
#                 data_to_csv2 = data_to_csv2.append({'query number': query_number, 'precision': precision,
#                                                     'precision@5': precision_5, 'precision@10': precision_10,
#                                                     'precision@50': precision_50, 'recall': recall, 'MAP': MAP},
#                                                    ignore_index=True)
#
#                 # line_number += 1
#         data_to_csv.to_csv('result.csv', index=False)
#         data_to_csv2.to_csv('metrics_result.csv', index=False)
