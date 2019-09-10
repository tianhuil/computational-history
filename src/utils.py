from glob import glob
from dask.delayed import delayed
import dask.dataframe as dd
import pandas as pd

def read_ngrams(glob_pattern):
  columns = ['ngram', 'year', 'total', 'distinct']
  dfs = [
    delayed(pd.read_csv)(filename, compression='gzip', sep='\t', header=None, names=columns)
    for filename in glob(glob_pattern)
  ]
  return dd.from_delayed(dfs)

def save_ngrams(df, output_dir):
  dfs = df.to_delayed()
  writes = [
    delayed(df.to_csv)(output_dir + '/{:05d}.gz'.format(k), compression='gzip', index=False)
    for k, df in enumerate(dfs)
  ]
  dd.compute(*writes)
