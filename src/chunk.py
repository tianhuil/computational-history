from itertools import islice
import gzip
import os
from glob import glob

def chunker(iterable, n):
  iterator = iter(iterable)
  while True:
    chunk = [x for x in islice(iterator, n)]
    if chunk:
      yield chunk
    else:
      break

def cat_file(filenames):
  for filename in filenames:
    yield from gzip.open(filename, 'rt')

def chunk_iters(filenames, chunksize):
  yield from chunker(cat_file(filenames), n=chunksize)

def write_chunks(filenames, output_dir, chunksize=int(1e6), overwrite=False):
  if not os.path.exists(output_dir):
    os.makedirs(output_dir)
  elif not overwrite and os.path.exists(output_dir):
    raise IOError("Director {} already exists".format(output_dir))
  else:  # overwrite and os.path.exists(output_dir)
    for filename in os.listdir(output_dir):
      os.remove(os.path.join(output_dir, filename))

  for i, chunk in enumerate(chunk_iters(sorted(filenames), chunksize=chunksize)):
    with gzip.open(output_dir + '/{:05d}.gz'.format(i), 'wt') as fh:
      for line in chunk:
        fh.write(line)

if __name__ == '__main__':
  GLOB = "/mnt/volume_sfo2_02/downloads/google_ngrams/5/googlebooks-eng-us-all-5gram-20120701-*.gz"
  OUTPUT_DIR = "/mnt/volume_sfo2_02/downloads/google_ngrams/5-part/"
  write_chunks(glob(GLOB), OUTPUT_DIR)
