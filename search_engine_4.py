import pickle
from indexer import Indexer
from parser_module import Parse
from reader import ReadFile
from searcher import Searcher

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
        # self._indexer.save_index('idx_bench.pkl')

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
        query_as_list = query.split(' ')
        expand_query(query_as_list)
        query = ' '.join(query_as_list)
        relevant_docs = searcher.search(query)
        return relevant_docs

    @property
    def indexer(self):
        return self._indexer


def expand_query(query):
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

# def merge_posting_files(dict1, dict2):
#     new = {key: dict1.get(key, []) + dict2.get(key, []) for key in set(list(dict1.keys()) + list(dict2.keys()))}
#     return new
