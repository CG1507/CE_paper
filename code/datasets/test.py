import datasets
from utils import io
import gzip
import json
import pickle

def parse(filepath):
	g = gzip.open(filepath, 'r')
	for l in g:
		yield eval(l)

def analyze_categories(data_folder, category_names):
	subcategories = {}
	for category in category_names:
		subcategories[category] = []

	for category in category_names:
		file_pointer = parse(data_folder + category + '/reviews.json.gz')
		for line in file_pointer:
			try:
				json_line = json.loads(json.dumps(line))
				name = json_line['reviewerName']
				print(name)
			except:
				pass

def test():
	data_folder = "/media/dell/Seagate Expansion Drive/CE_paper/Dataset/Amazon Dataset/categories/"
	category_names = io.list_dirs(data_folder)
	analyze_categories(data_folder, category_names)

if __name__ == "__main__":
	test()