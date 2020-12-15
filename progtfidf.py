#!/usr/bin/env python3

# Пункевич Роман м-110, Вариант №1 Реализовать алгоритм TF*IDF

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
from mrjob.protocol import RawValueProtocol
import math

import re

WORD_RE = re.compile(r"[\w']+")
SIZE = 4
terms_of_search = ["saint", "petersburg", "city"]

class MRWordFrequencyCount(MRJob):

	def steps(self):
        	return [
            		MRStep(
            			mapper=self.get_terms,
            			reducer=self.get_tf
            			),
            		MRStep(
            			mapper=self.get_docname_tf,
            			reducer=self.get_totalcount,
            			),
            		MRStep(
				mapper=self.get_termscount,
				reducer=self.get_tfidf,
				),
			MRStep(
				combiner=self.filter,
				reducer=self.list_of_doc,
				),
        	]

	def get_terms(self, _, line):
		docname = jobconf_from_env('map.input.file')
		for term in WORD_RE.findall(line):
            		yield ((term.lower(), docname), 1)
            		
	def get_tf(self, term_doc, count):
		term, docname = term_doc[0], term_doc[1]
		yield ((term, docname), sum(count))
	
	def get_docname_tf(self, term_doc, count):
		term, docname = term_doc[0], term_doc[1]
		yield (docname, (term, count))
		
	def get_totalcount(self, docname, term_count):
		total_count = 0
		terms = []
		for term, term_count in term_count:
			total_count += term_count
			terms.append((term, term_count))
		for term, term_count in terms:
			yield ((term, docname), (term_count, total_count))
					
	def get_termscount(self, term_docname, terms_total):
		term, docname = term_docname[0], term_docname[1]
		termcount_in_doc, totalterms_in_doc = terms_total[0], terms_total[1]
		yield (term, (docname, termcount_in_doc, totalterms_in_doc))

	def get_tfidf(self, term, docname_termcount_total):
		docname_termcount_total = list(docname_termcount_total)
		docsterm = len(docname_termcount_total)
		for docname, termcount_in_doc, terms_in_doc in docname_termcount_total:
			tfidf = (termcount_in_doc / terms_in_doc) * math.log(SIZE / docsterm)
			yield (term, (docname, tfidf))
			
	def filter(self, term, doc_tfidf):
		if term in terms_of_search:
			for docname, tfidf in doc_tfidf:
				yield docname, (term, tfidf)
				
	def list_of_doc(self, docname, term_tfidf):
		sum_tfidf = 0;
		for term, tfidf in term_tfidf:
			sum_tfidf += tfidf;
		sum_tfidf /= len(terms_of_search)
		yield docname, sum_tfidf 

if __name__ == '__main__':
    MRWordFrequencyCount.run()
