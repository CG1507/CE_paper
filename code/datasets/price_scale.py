import datasets
from utils import io
import gzip
import json
import pickle

def parse(filepath):
	g = gzip.open(filepath, 'r')
	for l in g:
		yield eval(l)

def get_price_scale(data_folder, category_names):
	for category in category_names:
		reading_meta_file_pointer = parse(data_folder + category + '/meta.json.gz')

def test():
	data_folder = "/media/dell/Seagate Expansion Drive/CE_paper/Dataset/Amazon Dataset/categories/"
	category_names = io.list_dirs(data_folder)
	get_price_scale(data_folder, category_names)
	
if __name__ == "__main__":
	test()