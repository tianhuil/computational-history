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

def process(df_raw):
  df = (pd.concat([
    df_raw[['query', w, 'year', 'total', 'distinct']].rename(columns={w: 'w'})
    for w in ['r1', 'r2', 'r3']
  ]).groupby(['w', 'query', 'year'])
      .sum()
      .reset_index())
  df['decade'] = df['year'] // 10 * 10
  return df


ARTICLES = [
    "article one",
    "article two",
    "article three",
    "article four",
    "article five",
    "article six",
    "article seven"
]

AMENDMENTS = [
    "first amendment",
    "second amendment",
    "third amendment",
    "fourth amendment",
    "fifth amendment",
    "sixth amendment",
    "seventh amendment",
    "eighth amendment",
    "ninth amendment",
    "tenth amendment",
    "eleventh amendment",
    "twelfth amendment",
    "thirteenth amendment",
    "fourteenth amendment",
    "fifteenth amendment",
    "sixteenth amendment",
    "seventeenth amendment",
    "eighteenth amendment",
    "nineteenth amendment",
    "twentieth amendment",
    "twenty-first amendment",
    "twenty-second amendment",
    "twenty-third amendment",
    "twenty-fourth amendment",
    "twenty-fifth amendment",
    "twenty-sixth amendment",
    "twenty-seventh amendment"
]

QUERIES = ARTICLES + AMENDMENTS

if __name__ == '__main__':
  assert [x for x in chunker(range(6), 2)] == [[0, 1], [2, 3], [4, 5]]
  assert [x for x in chunker(range(6), 4)] == [[0, 1, 2, 3], [4, 5]]
