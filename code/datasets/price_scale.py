import datasets
from utils import io
import gzip
import json
import pickle
import os

def create_dir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)

def parse(filepath):
	g = gzip.open(filepath, 'r')
	for l in g:
		yield eval(l)

def get_price_scale(data_folder, final_category_dir, category_names):
	all_categories = {}
	sum = {}
	for category in category_names:
		reading_meta_file_pointer = parse(data_folder + category + '/meta.json.gz')
		print('Reading ' + category + ' metadata...')

		for line in reading_meta_file_pointer:
			try:
				json_line = json.loads(json.dumps(line))
				related = json_line['related']
				salesrank = json_line['salesRank']
				brand = json_line['brand']
				categories = json_line['categories']
				asin = json_line['asin']
				price = json_line['price']
				json_dump = {}
				json_dump[asin] = price
				json_dump['price_level'] = 0
				for cat in categories:
					directory = final_category_dir + '/'.join(cat) + '/' + brand + '/'
					create_dir(directory)
					if directory in all_categories:
						writing_file_pointer = io.append_file(directory + 'price.json')
						io.write_line(writing_file_pointer, json.dumps(json_dump))
						all_categories[directory] += 1
						sum[directory] += price
						writing_file_pointer.close()
					else:
						writing_file_pointer = io.create_file(directory + 'price.json')
						io.write_line(writing_file_pointer, json.dumps(json_dump))
						all_categories[directory] = 1
						sum[directory] = price
						writing_file_pointer.close()
			except:
				pass

def test():
	data_folder = "/media/dell/Seagate Expansion Drive/CE_paper/Dataset/Amazon Dataset/categories/"
	final_category_dir = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/categories/'
	category_names = io.list_dirs(data_folder)
	get_price_scale(data_folder, final_category_dir, category_names)
	
if __name__ == "__main__":
	test()