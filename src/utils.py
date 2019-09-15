from itertools import islice
import io
import gzip
from glob import glob
from dask.delayed import delayed
import dask.dataframe as dd
import pandas as pd
import csv

def read_ngrams(glob_pattern):
  return dd.read_csv(
    glob_pattern,
    sep='\t',
    blocksize=None,
    compression='gzip',
    header=None,
    quoting=csv.QUOTE_NONE
  )

def save_ngrams(df, output_dir):
  dfs = df.to_delayed()
  writes = [
    delayed(df.to_csv)(output_dir + '/{:05d}.gz'.format(k), compression='gzip', index=False)
    for k, df in enumerate(dfs)
  ]
  dd.compute(*writes)

if __name__ == '__main__':
  assert [x for x in chunker(range(6), 2)] == [[0, 1], [2, 3], [4, 5]]
  assert [x for x in chunker(range(6), 4)] == [[0, 1, 2, 3], [4, 5]]
