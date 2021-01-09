import pandas as pd
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
from nltk.corpus import wordnet
import glob2


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
        print('Finished parsing and indexing.', 'inverted_index_len:', len(self._indexer.inverted_idx.keys()))
        self._indexer.save_index('idx_bench.pkl')


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
        add_to_query = {}
        for q in query_as_list:
            for syn in wordnet.synsets(q):
                for lemma in syn.lemmas():
                    if lemma.name() == q.lower():
                        continue
                    score = wordnet.synsets(q)[0].wup_similarity(syn)
                    if score is not None and score > 0.8:
                        add_to_query[lemma.name()] = score

        if len(add_to_query) > 3:
            add_to_query = sorted(add_to_query.items(), key=lambda item: item[1], reverse=True)
            query_as_list.extend([add_to_query[0][0], add_to_query[1][0], add_to_query[2][0]])
        else:
            query_as_list.extend(add_to_query)

        new_query = ' '.join(query_as_list)
        relevant_docs = searcher.search(new_query)

        return relevant_docs

    @property
    def indexer(self):
        return self._indexer


# def main():
    # config = ConfigClass()
    # path = config.get__corpusPath()
    # search_engine = SearchEngine(config)
    #
    # # files_in_folder = glob2.glob(path + '/**/*.parquet')
    # # for fp in files_in_folder:
    # #     search_engine.build_index_from_parquet(fp)
    #
    # # search_engine._indexer.save_index(config.saveFilesWithoutStem)
    #
    # search_engine.indexer.load_index(config.saveFilesWithoutStem)
    #
    # search_engine.search('healthy people should NOT wear masks')

# main()