import datasets
from utils import unixtime, io
import gzip
import json
import pickle
import os

def create_month_dirs(tmp_category_dir):
	if not os.path.exists(tmp_category_dir):
		os.makedirs(tmp_category_dir)

def write_reviews_by_date(file_pointer, category_names, tmp_category_dir):
	for category in category_names:
		line_no = 1
		all_date_strings = []

		for line in file_pointer[category]:
			try:
				json_line = json.loads(json.dumps(line))
				unixReviewTime = json_line["unixReviewTime"]
				date_time = unixtime.convert(unixReviewTime)
				date = unixtime.get_date(date_time)
				month = unixtime.get_month(date_time)
				year = unixtime.get_year(date_time)
				date_string = year + '_' + month + '_' + date

				if date_string in all_date_strings:
					current_file_pointer = io.append_file(tmp_category_dir + category + '/reviews_by_date/' + year + '/' + month + '/' + date + '.json')
					io.write_line(current_file_pointer, json.dumps(line) + '\n')
					current_file_pointer.close()
				else:
					create_month_dirs(tmp_category_dir + category + '/reviews_by_date/' + year + '/' + month + '/')
					current_file_pointer = io.create_file(tmp_category_dir + category + '/reviews_by_date/' + year + '/' + month + '/' + date + '.json')
					io.write_line(current_file_pointer, json.dumps(line) + '\n')
					all_date_strings.append(date_string)
					current_file_pointer.close()

				line_no += 1
			
			except:
				print('🐛 Error generated in reviews at ' + str(line_no) + ' in ' + category)
				pass
		
		print('Total number of reviews in ' + category + ' is ' + str(line_no))
		

def write_meta_chunks(file_pointer, category_names, tmp_category_dir):
	for category in category_names:
		chunks = {}
		line_no = 1
		chunk_no = 1
		chunk_break = 100000
		try:
			for line in file_pointer[category]:
					json_line = json.loads(json.dumps(line))
					asin = json_line['asin']
					chunks[asin] = json_line

					if line_no == chunk_break: 
						with open(tmp_category_dir + category  + '/meta_chunks/' + 'meta' + str(chunk_no) +'.pkl', 'wb') as f:
							pickle.dump(chunks, f)
							print(category + ': ' + 'meta'+ str(chunk_no) + ' saved')
						chunks = {}
						chunk_no += 1
						chunk_break += 100000
						
					line_no += 1

			with open(tmp_category_dir + category  + '/meta_chunks/' + 'meta' + str(chunk_no) +'.pkl', 'wb') as f:
				pickle.dump(chunks, f)
				print(category + ': ' + 'meta'+ str(chunk_no) + ' saved')
			print('Total number of products in ' + category + ' is ' + str(line_no))
		except:
			print('🐛 Error generated in ' + category + ', in meta' + str(chunk_no) + '.pkl at line no: ' + str(line_no))
			pass

def create_tmp_dirs(category_names, tmp_category_dir):
	io.make_dir(category_names, tmp_category_dir)
	for category in category_names:
		io.make_dir(['reviews_by_date', 'meta_chunks'], tmp_category_dir + category + '/')

def parse(filepath):
	g = gzip.open(filepath, 'r')
	for l in g:
		yield eval(l)

def get_file_pointer(dir_path, category_names):
	reviews_file_pointer = {}
	meta_file_pointer = {}

	for category in category_names:
		reviews_file_pointer[category] = parse(dir_path + category + "/reviews.json.gz")
		meta_file_pointer[category] = parse(dir_path + category+ "/meta.json.gz")
	return reviews_file_pointer, meta_file_pointer

def create_categories(data_folder, category_names = '', tmp_category_dir = '../../tmp_data/categories/'):
	if category_names == '':
		category_names = io.list_dirs(data_folder)
	reviews_file_pointer, meta_file_pointer = get_file_pointer(data_folder, category_names)
	create_tmp_dirs(category_names, tmp_category_dir)
	print('Following are the '+ str(len(category_names)) +' categories: \n', category_names)
	
	write_meta_chunks(meta_file_pointer, category_names, tmp_category_dir)
	write_reviews_by_date(reviews_file_pointer, category_names, tmp_category_dir)

def test():
	data_folder = "/media/dell/Seagate Expansion Drive/CE_paper/Dataset/Amazon Dataset/categories/"
	tmp_category_dir = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/categories/'
	#category_names = ["Apps for Android"]
	category_names = ''
	create_categories(data_folder, category_names, tmp_category_dir)

if __name__ == "__main__":
	test()