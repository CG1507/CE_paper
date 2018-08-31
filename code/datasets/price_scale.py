import datasets
from utils import io
from sklearn.cluster import KMeans
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
	all_info = {}
	for category in category_names:
		reading_meta_file_pointer = parse(data_folder + category + '/meta.json.gz')
		print('Reading ' + category + ' metadata...')

		prices = []
		for line in reading_meta_file_pointer:
			try:
				json_line = json.loads(json.dumps(line))
				asin = json_line['asin']
				price = json_line['price']
				prices.append(price)
			except:
				pass

		if len(prices) != 0:
			print('Clustering...')
			temp = []
			for item in prices:
				temp.append([item])

			kmeans = KMeans(n_clusters = 3, random_state = 0).fit(temp)
			labels = kmeans.labels_
			class0 = []
			class1 = []
			class2 = []
			
			for i in range(len(prices)):
				if labels[i] == 0:
					class0.append(prices[i])
					continue

				if labels[i] == 1:
					class1.append(prices[i])
					continue

				if labels[i] == 2:
					class2.append(prices[i])
					continue

			class0.sort()
			class1.sort()
			class2.sort()

			find_cat = []
			find_cat.append(class0[0])
			find_cat.append(class0[-1])
			find_cat.append(class1[0])
			find_cat.append(class1[-1])
			find_cat.append(class2[0])
			find_cat.append(class2[-1])
			find_cat.sort()

			info = {}
			if find_cat.index(class0[0]) in [0,1]:
				info['low'] = {'min': class0[0], 'max': class0[-1], 'number': len(class0)}
			elif find_cat.index(class0[0]) in [2,3]:
				info['med'] = {'min': class0[0], 'max': class0[-1], 'number': len(class0)}
			else:
				info['high'] = {'min': class0[0], 'max': class0[-1], 'number': len(class0)}

			if find_cat.index(class1[0]) in [0,1]:
				info['low'] = {'min': class1[0], 'max': class1[-1], 'number': len(class1)}
			elif find_cat.index(class1[0]) in [2,3]:
				info['med'] = {'min': class1[0], 'max': class1[-1], 'number': len(class1)}
			else:
				info['high'] = {'min': class1[0], 'max': class1[-1], 'number': len(class1)}

			if find_cat.index(class2[0]) in [0,1]:
				info['low'] = {'min': class2[0], 'max': class2[-1], 'number': len(class2)}
			elif find_cat.index(class2[0]) in [2,3]:
				info['med'] = {'min': class2[0], 'max': class2[-1], 'number': len(class2)}
			else:
				info['high'] = {'min': class2[0], 'max': class2[-1], 'number': len(class2)}
			
			all_info[category] = info
			print(info)

	info_file_pointer = open('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/info.pkl', 'wb')
	pickle.dump(all_info, info_file_pointer)

def test():
	data_folder = "/media/dell/Seagate Expansion Drive/CE_paper/Dataset/Amazon Dataset/categories/"
	final_category_dir = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/price_scale/categories/'
	category_names = io.list_dirs(data_folder)
	get_price_scale(data_folder, final_category_dir, category_names)
	
if __name__ == "__main__":
	test()