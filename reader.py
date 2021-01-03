import os

import glob2 as glob2
import pandas as pd


class ReadFile:
    def __init__(self):#, corpus_path):
        pass
        # self.corpus_path = corpus_path

    def read_file(self, file_name):
        """
        This function is reading a parquet file contains several tweets
        The file location is given as a string as an input to this function.
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        # full_path = os.path.join(self.corpus_path, file_name)
        df = pd.read_parquet(file_name, engine="pyarrow")
        return df.values.tolist()

    # def read_folder(self, folder_name):
    #     files_in_folder = glob2.glob(folder_name + '/**/*.parquet')
    #     df = pd.concat(pd.read_parquet(fp, engine="pyarrow") for fp in files_in_folder)
    #     return df.values.tolist()