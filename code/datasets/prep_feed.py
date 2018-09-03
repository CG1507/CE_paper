import datasets
from utils import io, unixtime
from sentencelabel import core
import gzip
import json
import pickle
import synch
import os

price_details_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/price.pkl'
with open(price_details_path, 'rb') as f:
	price_scale = pickle.load(f)

global_data = {
				'last_rank': 0, 
				'price_scale': price_scale, 
				'available_products': {}
			  }

def get_new_rank():
	global global_data
	global_data['last_rank'] += 1
	return global_data['last_rank']

def get_sentiment_scale(sentiment):
	if sentiment >= 0:
		return 1, 0
	else:
		return 0, 1

def get_price_scale(price, category):
	global global_data
	l_min = global_data['price_scale'][category]['low']['min']
	l_max = global_data['price_scale'][category]['low']['max']
	m_min = global_data['price_scale'][category]['med']['min']
	m_max = global_data['price_scale'][category]['med']['max']
	h_min = global_data['price_scale'][category]['high']['min']
	h_max = global_data['price_scale'][category]['high']['max']
	if price >= l_min and price <= l_max:
		return 0
	elif price >= m_min and price <= m_max:
		return 1
	else:
		return 2

def get_reviewer_details(tmp_category_dir, category, asin, helpful, reviewText, overall, price, related, salesRank, unixReviewTime, brand, categories):
	reviewer_filepath = tmp_category_dir + category + '/reviewers/' + asin + '.json'
	if io.file_presence(reviewer_filepath):
		reviewer_file_pointer = io.read_file(reviewer_filepath)
	else:
		reviewer_file_pointer = io.create_file(reviewer_filepath)
		sentiment, total_reacted, helpfulness, rating = get_review_details(helpful, reviewText, overall)
		reviewer_json = {}
		reviewer_json['rank'] = get_new_rank()
		reviewer_json['#_reviews'] = 1
		reviewer_json['#_+ve_reviews'], reviewer_json['#_-ve_reviews'] = get_sentiment_scale(sentiment)
		reviewer_json['total_reacted'] = total_reacted
		reviewer_json['helpfulness'] = helpfulness
		reviewer_json['rating'] = rating
		reviewer_json['price'] = get_price_scale(price, category)
		date, month, year, day = unix_to_attributes(unixReviewTime)
		reviewer_json['first_purchase'] = [date, month, year]
		reviewer_json['engaged_time'] = 0
		reviewer_json['reviews'] = {asin: {'#_time': 1, 'category': category, 'subcategory': categories, 'brand': brand}}


def get_review_details(helpful, reviewText, overall):
	sentiment = core.get_sentiment(reviewText, on_base = "t", flag_prob=False)
	total_reacted = helpful[1]
	helpfulness = helpful[0]
	rating = overall
	return sentiment, total_reacted, helpfulness, rating

def unix_to_attributes(unixReviewTime):
	date_time = unixtime.convert(unixReviewTime)
	date = get_date(date_time)
	month = get_month(date_time)
	year = get_year(date_time)
	day = get_day(date_time)

	return int(date), int(month), int(year), int(day)

def no_products_related(related):
	no_products = {}
	no_products["also_bought"] = len(related["also_bought"])
	no_products["also_viewed"] = len(related["also_viewed"])
	no_products["bought_together"] = len(related["bought_together"])
	return no_products

def get_influential_attributes(available_products):
	global global_data
	influential_attributes = {}
	influential_attributes['#_reviews'] = 0
	influential_attributes['#_+ve_reviews'] = 0
	influential_attributes['#_-ve_reviews'] = 0
	influential_attributes['total_reacted'] = 0
	influential_attributes['helpfulness'] = 0
	influential_attributes['rating'] = json_line0
	influential_attributes['price'] = 0
	influential_attributes['engaged_time'] = 0
	influential_attributes['#_products_related'] = 0
	influential_attributes['salerank'] = 0

	for product in available_products:
		reading_file_pointer = io.read_file(global_data['available_products'][product])
		line = reading_file_pointer.readline()
		json_line = json.loads(line)

		influential_attributes['#_reviews'] += json_line['#_reviews']
		influential_attributes['#_+ve_reviews'] += json_line['#_+ve_reviews']
		influential_attributes['#_-ve_reviews'] += json_line['#_-ve_reviews']
		influential_attributes['total_reacted'] += json_line['total_reacted']
		influential_attributes['helpfulness'] += json_line['helpfulness']
		influential_attributes['rating'] += json_line['rating']
		influential_attributes['price'] += json_line['price']
		influential_attributes['engaged_time'] += json_line['engaged_time']
		influential_attributes['#_products_related'] += json_line['#_products_related']
		influential_attributes['salerank'] += json_line['salerank']

	return influential_attributes

def influential_details(related):
	global global_data

	available_products = []
	for product in related['also_bought']:
		if product in global_data['available_products'].keys():
			available_products.append(product)

	available_products = []
	for product in related['also_viewed']:
		if product in global_data['available_products'].keys():
			available_products.append(product)


	available_products = []
	for product in related['bought_together']:
		if product in global_data['available_products'].keys():
			available_products.append(product)

def create_dir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)

def get_product_details(tmp_category_dir, category, asin, sentiment, total_reacted, helpfulness, rating, summary, unixReviewTime, title, price, related, salesRank, brand, categories):
	"""
	'# of reviews' : 2
	'# of +ve reviews' : 1
	'# of -ve reviews' : 1
	'Total of helpfulness people reacted' : 5
	'# of helpfulness' : 2
	'Overall ratings' : 5
	'price' : 0
	'# of times same person buy' : 2
	'first time buy @' : 20/9/2000
	'selling time' : current date - 'first time buy @'
	'# product in related' : 20
	'salerank' : 1212
	'all_categories': [[]]
	[influentail details]
	"""
	global global_data

	file_path_dir = tmp_category_dir + category + '/products/' + '/'.join(categories[0]) + '/'
	create_dir(file_path_dir)
	
	file_path = file_path_dir + asin + '.json'

	if asin not in global_data['available_products']:
		global_data['available_products'][asin] = file_path

	if io.file_presence(file_path):

	else:
		product_json = {}
		product_json['#_reviews'] = 1
		product_json['#_+ve_reviews'], product_json['#_-ve_reviews'] = get_sentiment_scale(sentiment)
		product_json['total_reacted'] = total_reacted
		product_json['helpfulness'] = helpfulness
		product_json['rating'] = rating
		product_json['price'] = get_price_scale(price, category)
		date, month, year, day = unix_to_attributes(unixReviewTime)
		product_json['first_purchase'] = [date, month, year]
		product_json['engaged_time'] = 0
		product_json['#_products_related'] = no_products_related(related)
		product_json['salerank'] = salesRank
		product_json['all_categories'] = categories

def get_attributes(json_line):
	FLAG = True

	if 'asin' in json_line:
		asin = json_line['asin']
	else:
		asin = ''

	if 'helpful' in json_line:
		helpful = json_line['helpful']
	else:
		helpful = [0, 0]

	if 'reviewText'	in json_line:
		reviewText = json_line['reviewText']
	else:
		reviewText = ''

	if 'overall' in json_line:
		overall = json_line['overall']
	else:
		overall = 0.0

	if 'summary' in json_line:
		summary = json_line['summary']
	else:
		summary = ''

	if 'unixReviewTime' in json_line:
		unixReviewTime = json_line['unixReviewTime']
	else:
		unixReviewTime = ''

	if 'title' in json_line:
		title = json_line['title']
	else:
		title = ''

	if 'price' in json_line:
		price = json_line['price']
	else:
		FLAG = False
		price = ''

	if 'related' in json_line:
		related = json_line['related']
		if "also_bought" not in related:
			related["also_bought"] = []

		if "also_viewed" not in related:
			related["also_viewed"] = []

		if "bought_together" not in related:
			related["bought_together"] = []
	else:
		related = {"also_bought": [], "also_viewed": [], "bought_together": []}

	if 'salesRank' in json_line:
		if json_line['salesRank'] != {}:
			salesRank = json_line['salesRank']
		else:
			FLAG = False
			salesRank = ''	
	else:
		FLAG = False
		salesRank = ''

	if 'brand' in json_line:
		brand = json_line['brand']
	else:
		FLAG = False
		brand = ''

	if 'categories' in json_line:
		categories = json_line['categories']
	else:
		FLAG = False
		categories = [[]]

	return FLAG, asin, helpful, reviewText, overall, summary, unixReviewTime, title, price, related, salesRank, brand, categories

def get_pickle_object(file_path):
	with open(file_path, 'rb') as f:
		return pickle.load(f)

def synch_data(paths, tmp_category_dir):
	prev_category = ''
	index = {}
	index_file_pointer = None

	path_no = 1
	for path in paths:
		category = path[1]
		file_path = path[0]
		meta_dir_path = tmp_category_dir + category + '/meta_chunks/'
		
		same_category_flag = True
		if prev_category != category:
			index_file_pointer = open(meta_dir_path + 'index.pkl', 'rb')
			index = pickle.load(index_file_pointer)
			same_category_flag = False
		prev_category = category

		prev_meta_file_no = ''
		meta = {}

		reading_file_pointer = io.read_file(file_path)
		line_no = 1
		for line in reading_file_pointer:
			try:
				json_line = json.loads(line)
				asin = json_line['asin']
				meta_file_no = str(index[asin])

				if prev_meta_file_no != meta_file_no:
					meta = get_pickle_object(meta_dir_path + 'meta' + meta_file_no + '.pkl')
				else:
					if not same_category_flag:
						meta = get_pickle_object(meta_dir_path + 'meta' + meta_file_no + '.pkl')
				prev_meta_file_no = meta_file_no
				
				product_json = meta[asin]
				merged_json = dict(json_line, **product_json)
				FLAG, asin, helpful, reviewText, overall, summary, unixReviewTime, title, price, related, salesRank, brand, categories = get_attributes(merged_json)
				
				if FLAG:
					print('Accept')
					#get csv line
				else:
					print('Till now reject')

				line_no += 1
				same_category_flag = True
			except:
				print('🐛 ERROR:\nFILE_PATH:', file_path, '\nLINE_NO:', line_no, '\nLINE:', line)
				pass
		print('Path no.', path_no, 'DONE:', file_path)
		path_no += 1

def create_dirs(tmp_category_dir, category_names):
	for category in category_names:
		io.make_dir(['reviewers', 'products'], tmp_category_dir + category + '/')

def write_feed_data(tmp_category_dir, category_names):
	paths = synch.get_sorted_files_path(tmp_category_dir, category_names)
	print('Total number of files:', len(paths))
	create_dirs(tmp_category_dir, category_names)
	synch_data(paths, tmp_category_dir)

def test():
	tmp_category_dir = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/categories/'
	category_names = ['Electronics']
	write_feed_data(tmp_category_dir, category_names)

if __name__ == "__main__":
	test()