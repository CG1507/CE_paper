import datasets
from utils import io
import gzip
import json
import pickle

def parse(filepath):
	g = gzip.open(filepath, 'r')
	for l in g:
		yield eval(l)

def test():
	
	
if __name__ == "__main__":
	test()