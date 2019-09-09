SHELL := /bin/bash
ACTIVATE := source env/bin/activate

create:
	python3 -m venv env

install:
	$(ACTIVATE) && pip3 install -r requirements.txt

ipython:
	$(ACTIVATE) && ipython --pdb

jupyter:
	$(ACTIVATE) && jupyter notebook

download:
	$(ACTIVATE) && google-ngram-downloader download -l eng-us -n 5 -v
	cd download && wget https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/simplewiki/20190820/simplewiki-20190820-pages-articles-multistream.xml.bz2
