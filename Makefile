SHELL := /bin/bash
ACTIVATE := source env/bin/activate
DATA_DIR := /mnt/volume_sfo2_02/

create:
	python3 -m venv env

install:
	$(ACTIVATE) && pip3 install -r requirements.txt

ipython:
	$(ACTIVATE) && ipython --pdb

jupyter:
	$(ACTIVATE) && jupyter lab

download:
	$(ACTIVATE) && google-ngram-downloader download -l eng-us -n 5 -v -o $(DATA_DIR)/downloads/google_ngrams/5/ 2> logs/log.5.txt
	$(ACTIVATE) && google-ngram-downloader download -l eng-us -n 1 -v -o $(DATA_DIR)/downloads/google_ngrams/1/ 2> logs/log.1.txt
	cd $(DATA_DIR) && wget http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-us-all-totalcounts-20120701.txt
	cd $(DATA_DIR) && wget https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/simplewiki/20190820/simplewiki-20190820-pages-articles-multistream.xml.bz2

start-spark:
	start-all.sh

stop-spark:
	stop-all.sh

spark-shell:
	spark-shell --master spark://tianhuil:7077
