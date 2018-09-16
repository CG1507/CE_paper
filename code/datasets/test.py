import datasets
from utils import io
from sentencelabel import core
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

	not_price = {}
	line_no = {}
	for category in category_names:
		file_pointer = parse(data_folder + category + '/reviews.json.gz')
		not_price[category] = 0
		line_no[category] = 0
		for line in file_pointer:
			try:
				json_line = json.loads(json.dumps(line))
				cat = json_line['reviewerID']
				print(cat)
			except:
				not_price[category] += 1
				line_no[category] += 1
				pass
			line_no[category] +=1
		print(category)
		print(line_no[category])
		print(not_price[category])

def see_data(data_folder, category_names):
	for category in category_names:
		file_pointer = parse(data_folder + category + '/meta.json.gz')
		for line in file_pointer:
			try:
				json_line = json.loads(json.dumps(line))
				cat = json_line['categories']
				print(cat)
				#keys = len(json_line.keys())
				#print(keys)
			except:
				#print('skipped')
				pass

def see_result():
	reading_file_pointer = io.read_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0_norm.csv')
	
	line_no = 1
	for line in reading_file_pointer:
		if line_no <= 3:
			print(line)
		else:
			break
		line_no += 1

def test():
	data_folder = "/media/dell/Seagate Expansion Drive/CE_paper/Dataset/Amazon Dataset/categories/"
	#category_names = io.list_dirs(data_folder)
	#category_names = ['Electronics']
	#see_data(data_folder, category_names)
	#analyze_categories(data_folder, category_names)
	#result = core.get_sentiment('I love you!', on_base = "t", flag_prob=True)
	#print(result)
	see_result()

if __name__ == "__main__":
	test()