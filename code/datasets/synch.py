import datasets
from utils import io
import gzip
import json
import pickle

def get_pickle_object(file_path):
	with open(file_path, 'rb') as f:
		return pickle.load(f)

def find_product_json(asin, tmp_category_dir, category):
	meta_dir_path = tmp_category_dir + category + '/meta_chunks/'
	meta_chunks_files = io.list_dirs(meta_dir_path)
	for file in meta_chunks_files:
		meta = get_pickle_object(meta_dir_path + file)
		try:
			product_json = meta[asin]
			return product_json
		except:
			pass
	return None

def synch_data(paths, tmp_category_dir):
	merged_categories_files = []
	all_categories_one_file_pointer = io.create_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/merged_sorted_reviews.json')
	
	no_of_meta = {}
	category_names = io.list_dirs(tmp_category_dir)
	for category in category_names:
		no_of_meta[category] = len(io.list_dirs(tmp_category_dir + category + '/meta_chunks/'))

	prev_category = ''
	meta_objects = []

	path_no = 1
	for path in paths:
		category = path[1]
		file_path = path[0]
		
		if prev_category != category:
			meta_objects = []
		meta_dir_path = tmp_category_dir + category + '/meta_chunks/'
		prev_category = category

		reading_file_pointer = io.read_file(file_path)
		line_no = 1
		for line in reading_file_pointer:
			try:
				json_line = json.loads(line)
				asin = json_line['asin']
				#------------------------------------------------------------------------
				product_json = None
				if no_of_meta[category] <= 6:
					for meta in meta_objects:
						if product_json == None:
							try:
								product_json = meta[asin]
							except:
								product_json = None
								pass
						else:
							break

					if len(meta_objects) < no_of_meta[category] and product_json == None:
						for i in range(no_of_meta[category]):
							if (i + 1) > len(meta_objects):
								if product_json == None:
									try:
										meta_objects.append(get_pickle_object(meta_dir_path + 'meta' + str(i + 1) + '.pkl'))
										product_json = meta_objects[i][asin]
									except:
										product_json = None
										pass
								else:
									break
				else:
					product_json = find_product_json(asin, tmp_category_dir, category)
				#------------------------------------------------------------------------
				if product_json == None:
					print('NOT FOUND', '\ncategory: ', category, '\nfile: ', file_path, '\nasin: ', asin)
				else:
					merged_json = dict(json_line, **product_json)
					if category in merged_categories_files:
						current_file_pointer = io.append_file(tmp_category_dir + category + '/merged.json')
						io.write_line(current_file_pointer, json.dumps(merged_json) + '\n')
						io.write_line(all_categories_one_file_pointer, json.dumps(merged_json) + '\n')
					else:
						current_file_pointer = io.append_file(tmp_category_dir + category + '/merged.json')
						io.write_line(current_file_pointer, json.dumps(merged_json) + '\n')
						io.write_line(all_categories_one_file_pointer, json.dumps(merged_json) + '\n')
						merged_categories_files.append(category)
				line_no += 1
			except:
				print('🐛 ERROR:\nFILE_PATH:', file_path, '\nLINE_NO:', line_no, '\nLINE:', line)
				pass
		print('Path no.', path_no, 'DONE:', file_path)
		path_no += 1

def get_sorted_files_path(tmp_category_dir):
	total_years = ['1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014'] 
	total_months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
	total_dates = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
	category_names  = io.list_dirs(tmp_category_dir)
	paths = []
	
	category_date_structure ={}
	for category in category_names:
		category_years = io.list_dirs(tmp_category_dir + category + '/reviews_by_date/')
		year_dirs = {}
		for year in category_years:
			category_months = io.list_dirs(tmp_category_dir + category + '/reviews_by_date/' + year)
			month_dirs={}
			for month in category_months:
				category_dates = io.list_files(tmp_category_dir + category + '/reviews_by_date/' + year + '/' + month + '/', ends_with = ".json")
				month_dirs[month] = category_dates
			year_dirs[year] = month_dirs
		category_date_structure[category] = year_dirs

	for year in total_years:
		for month in total_months:
			for date in total_dates:
				for category in category_names:
					if year in category_date_structure[category]:
						if month in category_date_structure[category][year]:
							if date in category_date_structure[category][year][month]:
								paths.append([tmp_category_dir + category + '/reviews_by_date/' + year + '/' + month + '/' + date + '.json', category])
	return paths

def write_merged_data(tmp_category_dir):
	paths = get_sorted_files_path(tmp_category_dir)
	print('No. of files:', len(paths))
	synch_data(paths, tmp_category_dir)

def test():
	tmp_category_dir = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/categories/'
	write_merged_data(tmp_category_dir)

if __name__ == "__main__":
	test()