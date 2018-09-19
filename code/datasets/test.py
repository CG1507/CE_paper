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
	reading_file_pointer = io.read_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/validation_dataset_0_norm.csv')
	
	line_no = 1
	for line in reading_file_pointer:
		att = line.split(',')
		print(att[3], att[4], att[5])

def create_small_data():
	reading_file_pointer = io.read_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0_norm.csv')
	writing_train_file_pointer = io.create_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/small_train_data.csv')
	writing_validation_file_pointer = io.create_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/small_validation_data.csv')
	line_no = 1
	for line in reading_file_pointer:
		if line_no <= 10000:
			io.write_line(writing_train_file_pointer, line)
		elif line_no > 10000 and line_no <= 15000: 
			io.write_line(writing_validation_file_pointer, line)
		else:
			break
		line_no += 1
	reading_file_pointer.close()
	writing_train_file_pointer.close()
	writing_validation_file_pointer.close()

def divide_data():
	reading_file_pointer = io.read_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0_norm.csv')
	writing_train_file_pointer = io.create_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/train_dataset_0_norm.csv')
	writing_validation_file_pointer = io.create_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/validation_dataset_0_norm.csv')
	line_no = 1
	for line in reading_file_pointer:

		if line_no <= 350000:
			io.write_line(writing_train_file_pointer, line) 
		else:
			io.write_line(writing_validation_file_pointer, line)
		line_no += 1
	reading_file_pointer.close()
	writing_train_file_pointer.close()
	writing_validation_file_pointer.close()

def test():
	data_folder = "/media/dell/Seagate Expansion Drive/CE_paper/Dataset/Amazon Dataset/categories/"
	#category_names = io.list_dirs(data_folder)
	#category_names = ['Electronics']
	#see_data(data_folder, category_names)
	#analyze_categories(data_folder, category_names)
	#result = core.get_sentiment('I love you!', on_base = "t", flag_prob=True)
	#print(result)
	see_result()
	#create_small_data()
	#divide_data()

if __name__ == "__main__":
	test()