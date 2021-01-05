import os

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
        self._indexer.save_index('indverted_idx.pkl')
        print('Finished parsing and indexing.')

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
    def load_precomputed_model(self):
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

    files_in_folder = glob2.glob(path + '/**/*.parquet')
    for fp in files_in_folder:
        search_engine.build_index_from_parquet(fp)

    search_engine.indexer.load_index('indverted_idx.pkl')

    #write ranked result to csv
    with open('queries.txt', encoding="utf8") as f:
        # line_number = 0
        for line in f.readlines():
            if line != '\n':
                line_number = line[0]
                line = line[1:]
                for doc_tuple in search_engine.search(line):
                    data_to_csv = data_to_csv.append({'query number': line_number, 'tweet id': doc_tuple[0], 'Rank': doc_tuple[1]}, ignore_index=True)

                #check result
                df = pd.read_csv(config.get__corpusPath() + 'benchmark_lbls_train.csv')
                metrics.precision(df, True, line_number)
                metrics.precision_at_n(df, line_number, 5)
                metrics.precision_at_n(df, line_number, 10)
                metrics.precision_at_n(df, line_number, 50)
                metrics.recall(df,)
                # line_number += 1
        data_to_csv.to_csv('result.csv', index=False)


    metrics.precision_at_n(config.get__corpusPath() + 'benchmark_lbls_train.csv', )

