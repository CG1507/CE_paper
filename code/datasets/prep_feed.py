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
				'price_scale': price_scale, 
				'available_products': {},
				'available_reviewers': {},
				'unavailable_products': {},
				'available_brands': {},
				'available_subcategories': {},
				'available_categories': {}
			  }

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
		return 1
	elif price >= m_min and price <= m_max:
		return 2
	else:
		return 3

def get_product_json(asin):
	global global_data
	reading_product_file_pointer = io.read_file(global_data['available_products'][asin])
	line = reading_product_file_pointer.readline()
	product_json = json.loads(line)
	reading_product_file_pointer.close()
	return product_json

def reviewers_no_products_related(products):
	reviewers_no_related = {}
	reviewers_no_related['also_bought'] = 0
	reviewers_no_related['also_viewed'] = 0
	reviewers_no_related['bought_together'] = 0

	for product in products:
		product_json = get_product_json(product)
		reviewers_no_related['also_bought'] += product_json['#_products_related']['also_bought']
		reviewers_no_related['also_viewed'] += product_json['#_products_related']['also_viewed']
		reviewers_no_related['bought_together'] += product_json['#_products_related']['bought_together']
	return reviewers_no_related

def add_review_to_reviewer(reviewer_json, asin, category, categories, brand):
	if asin in reviewer_json['reviews']:
		reviewer_json['reviews'][asin]['#_time'] += 1
	else:
		reviewer_json['reviews'][asin]['#_time'] = 1
		reviewer_json['reviews'][asin]['category'] = category
		reviewer_json['reviews'][asin]['subcategory'] = categories
		reviewer_json['reviews'][asin]['brand'] = brand
	return reviewer_json['reviews']

def buy_again(reviewer_json, asin):
	if asin in reviewer_json['reviews']:
		return 1
	else:
		return 0

def get_favourite(reviewer_json):
	category = {}
	subcategory = {}
	brand = {}
	
	for product in reviewer_json['reviews']:
		category[reviewer_json['reviews'][product]['category']] = 0
		subcategory[reviewer_json['reviews'][product]['subcategory']] = 0
		brand[reviewer_json['reviews'][product]['brand']] = 0

	for product in reviewer_json['reviews']:
		category[reviewer_json['reviews'][product]['category']] += reviewer_json['reviews'][product]['#_time']
		subcategory[reviewer_json['reviews'][product]['subcategory']] += reviewer_json['reviews'][product]['#_time']
		brand[reviewer_json['reviews'][product]['brand']] += reviewer_json['reviews'][product]['#_time']

	return sorted(category, key=category.get, reverse=True)[0], sorted(subcategory, key=subcategory.get, reverse=True)[0], sorted(brand, key=brand.get, reverse=True)[0]

def get_r_b_s_c_influential_details(asin, also_bought, also_viewed, bought_together):
	influential_attributes = {}
	influential_attributes['also_bought'] = also_bought
	influential_attributes['also_viewed'] = also_viewed
	influential_attributes['bought_together'] = bought_together

	product_json = get_product_json(asin)
	mapping = {'also_bought': product_json['also_bought_influential'], 'also_viewed': product_json['also_viewed_influential'], 'bought_together': product_json['bought_together_influential']}
	for i in mapping:
		influential_attributes[i]['#_reviews'] += mapping[i]['#_reviews']
		influential_attributes[i]['#_+ve_reviews'] += mapping[i]['#_+ve_reviews']
		influential_attributes[i]['#_-ve_reviews'] += mapping[i]['#_-ve_reviews']
		influential_attributes[i]['total_reacted'] += mapping[i]['total_reacted']
		influential_attributes[i]['helpfulness'] += mapping[i]['helpfulness']
		influential_attributes[i]['rating'] += mapping[i]['rating']
		influential_attributes[i]['price'] += mapping[i]['price']
		influential_attributes[i]['engaged_time'] += mapping[i]['engaged_time']
		influential_attributes[i]['#_products_related'] += mapping[i]['#_products_related']
		influential_attributes[i]['salerank'] += mapping[i]['salerank']
		influential_attributes[i]['buy_again'] += mapping[i]['buy_again']

	return influential_attributes['also_bought'], influential_attributes['also_viewed'], influential_attributes['bought_together']

def get_reviewer_details(tmp_category_dir, category, reviewerID, asin, sentiment, total_reacted, helpfulness, rating, summary, date, month, year, day, title, price, related, salesRank, brand, categories):
	global global_data

	reviewer_filepath = tmp_category_dir + category + '/reviewers/' + reviewerID + '.json'

	if reviewerID in global_data['available_reviewers']:
		reading_reviewer_file_pointer = io.read_file(reviewer_filepath)
		line = reading_reviewer_file_pointer.readline()
		reading_reviewer_file_pointer.close()
		reviewer_json = json.loads(line)

		reviewer_json['#_reviews'] += 1
		pos_senti, neg_senti = get_sentiment_scale(sentiment)
		reviewer_json['#_+ve_reviews'] += pos_senti
		reviewer_json['#_-ve_reviews'] += neg_senti
		reviewer_json['total_reacted'] += total_reacted
		reviewer_json['helpfulness'] += helpfulness
		reviewer_json['rating'] += rating
		reviewer_json['price'] += get_price_scale(price, category)
		reviewer_json['engaged_time'] = unixtime.days_difference(reviewer_json['first_purchase'], [year, month, date])
		buy_again_value = buy_again(reviewer_json, asin)
		if buy_again_value == 0:
			reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'])
		reviewer_json['buy_again'] += buy_again_value
		reviewer_json['reviews'] = add_review_to_reviewer(reviewer_json, asin, category, categories, brand)
		reviewer_json['#_products_related'] = reviewers_no_products_related(reviewer_json['reviews'].keys())
		reviewer_json['fav_category'], reviewer_json['fav_subcategory'], reviewer_json['fav_brand'] = get_favourite(reviewer_json)
	
		writing_reviewer_file_pointer = io.create_file(reviewer_filepath)
		io.write_line(writing_reviewer_file_pointer, json.dumps(reviewer_json))
		writing_reviewer_file_pointer.close()

	else:
		global_data['available_reviewers'][reviewerID] = reviewer_filepath
		
		product_json = get_product_json(asin)
		reviewer_json = {}
		reviewer_json['#_reviews'] = 1
		reviewer_json['#_+ve_reviews'], reviewer_json['#_-ve_reviews'] = get_sentiment_scale(sentiment)
		reviewer_json['total_reacted'] = total_reacted
		reviewer_json['helpfulness'] = helpfulness
		reviewer_json['rating'] = rating
		reviewer_json['price'] = get_price_scale(price, category)
		reviewer_json['first_purchase'] = [year, month, date]
		reviewer_json['engaged_time'] = 0
		reviewer_json['buy_again'] = 0
		reviewer_json['reviews'] = {asin: {'#_time': 1, 'category': category, 'subcategory': categories, 'brand': brand}}
		reviewer_json['#_products_related'] = no_products_related(related)
		reviewer_json['fav_category'] = category
		reviewer_json['fav_subcategory'] = ''
		reviewer_json['fav_brand'] = brand
		reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'] = product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential']
		
		writing_reviewer_file_pointer = io.create_file(reviewer_filepath)
		io.write_line(writing_reviewer_file_pointer, json.dumps(reviewer_json))
		writing_reviewer_file_pointer.close()

def get_brand_details(tmp_category_dir, category, reviewerID, asin, sentiment, total_reacted, helpfulness, rating, summary, date, month, year, day, title, price, related, salesRank, brand, categories):
	global global_data

	brand_filepath = tmp_category_dir + category + '/brands/' + brand + '.json'

	if brand in global_data['available_brands']:
		reading_brand_file_pointer = io.read_file(brand_filepath)
		line = reading_brand_file_pointer.readline()
		reading_brand_file_pointer.close()
		brand_json = json.loads(line)

		brand_json['#_reviews'] += 1
		pos_senti, neg_senti = get_sentiment_scale(sentiment)
		brand_json['#_+ve_reviews'] += pos_senti
		brand_json['#_-ve_reviews'] += neg_senti
		brand_json['total_reacted'] += total_reacted
		brand_json['helpfulness'] += helpfulness
		brand_json['rating'] += rating
		brand_json['price'] += get_price_scale(price, category)
		brand_json['engaged_time'] = unixtime.days_difference(brand_json['first_purchase'], [year, month, date])
		if asin not in brand_json['products']:
			brand_json['#_products'] += 1
			brand_json['products'].append([asin])
			brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'])
		brand_json['#_products_related'] = reviewers_no_products_related(brand_json['reviews'].keys())
		
		writing_brand_file_pointer = io.create_file(brand_filepath)
		io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
		writing_brand_file_pointer.close()

	else:
		global_data['available_brands'][brand] = brand_filepath
		
		product_json = get_product_json(asin)
		brand_json = {}
		brand_json['#_reviews'] = 1
		brand_json['#_products'] = 1
		brand_json['#_+ve_reviews'], brand_json['#_-ve_reviews'] = get_sentiment_scale(sentiment)
		brand_json['total_reacted'] = total_reacted
		brand_json['helpfulness'] = helpfulness
		brand_json['rating'] = rating
		brand_json['price'] = get_price_scale(price, category)
		brand_json['first_purchase'] = [year, month, date]
		brand_json['engaged_time'] = 0
		brand_json['products'] = [asin]
		brand_json['#_products_related'] = no_products_related(related)
		brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential']
		
		writing_brand_file_pointer = io.create_file(brand_filepath)
		io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
		writing_brand_file_pointer.close()


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
	
	count = 0 
	for product in related["also_bought"]:
		if product in global_data['available_products']:
			count += 1
	no_products["also_bought"] = count
			
	count = 0 
	for product in related["also_viewed"]:
		if product in global_data['available_products']:
			count += 1
	no_products["also_viewed"] = count
	
	count = 0
	for product in related["bought_together"]:
		if product in global_data['available_products']:
			count += 1
	no_products["bought_together"] = count
	return no_products

def repeated_purchase(tmp_category_dir, category, reviewerID, asin):
	reading_reviewer_file_pointer = io.read_file(tmp_category_dir + category + '/reviewers/' + asin + '.json')
	line = reading_file_pointer.readline()
	reviewer_json = json.loads(line)
	reading_reviewer_file_pointer.close()
	if 'reviews' in reviewer_json:
		if asin in reviewer_json['reviews']:
			return 1
	return 0

def get_influential_attributes(available_products):
	global global_data
	influential_attributes = {}
	influential_attributes['#_reviews'] = 0
	influential_attributes['#_+ve_reviews'] = 0
	influential_attributes['#_-ve_reviews'] = 0
	influential_attributes['total_reacted'] = 0
	influential_attributes['helpfulness'] = 0
	influential_attributes['rating'] = 0
	influential_attributes['price'] = 0
	influential_attributes['engaged_time'] = 0
	influential_attributes['#_products_related'] = 0
	influential_attributes['salerank'] = 0
	influential_attributes['buy_again'] = 0

	for product in available_products:
		reading_file_pointer = io.read_file(global_data['available_products'][product])
		line = reading_file_pointer.readline()
		product_json = json.loads(line)

		influential_attributes['#_reviews'] += product_json['#_reviews']
		influential_attributes['#_+ve_reviews'] += product_json['#_+ve_reviews']
		influential_attributes['#_-ve_reviews'] += product_json['#_-ve_reviews']
		influential_attributes['total_reacted'] += product_json['total_reacted']
		influential_attributes['helpfulness'] += product_json['helpfulness']
		influential_attributes['rating'] += product_json['rating']
		influential_attributes['price'] += product_json['price']
		influential_attributes['engaged_time'] += product_json['engaged_time']
		influential_attributes['#_products_related'] += product_json['#_products_related']
		influential_attributes['buy_again'] += product_json['buy_again']
		influential_attributes['salerank'] += product_json['salerank']
		reading_file_pointer.close()

	return influential_attributes

def get_influential_details(related, asin):
	global global_data

	available_products = []
	for product in related['also_bought']:
		if product in global_data['available_products']:
			available_products.append(product)
		else:
			if product in global_data['unavailable_products']:
				if asin not in global_data['unavailable_products'][product]:
					global_data['unavailable_products'][product].append(asin)
			else:
				global_data['unavailable_products'][product] = [asin]
	also_bought_influential = get_influential_attributes(available_products)

	available_products = []
	for product in related['also_viewed']:
		if product in global_data['available_products']:
			available_products.append(product)
		else:
			if product in global_data['unavailable_products']:
				if asin not in global_data['unavailable_products'][product]:
					global_data['unavailable_products'][product].append(asin)
			else:
				global_data['unavailable_products'][product] = [asin]
	also_viewed_influential = get_influential_attributes(available_products)

	available_products = []
	for product in related['bought_together']:
		if product in global_data['available_products']:
			available_products.append(product)
		else:
			if product in global_data['unavailable_products']:
				if asin not in global_data['unavailable_products'][product]:
					global_data['unavailable_products'][product].append(asin)
			else:
				global_data['unavailable_products'][product] = [asin]
	bought_together_influential = get_influential_attributes(available_products)

	return also_bought_influential, also_viewed_influential, bought_together_influential

def create_dir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)

def update_no_products_related(product):
	global global_data
	product_json = get_product_json(product)
	related = product_json['related']
	product_json['#_products_related'] = no_products_related(related)
	writing_product_file_pointer = io.create_file(global_data['available_products'][product])
	io.write_line(writing_product_file_pointer, json.dumps(product_json))
	writing_product_file_pointer.close()

def update_influential_details(product):
	global global_data
	product_json = get_product_json(product)
	related = product_json['related']
	product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential'] = get_influential_details(related, product)
	writing_product_file_pointer = io.create_file(global_data['available_products'][product])
	io.write_line(writing_product_file_pointer, json.dumps(product_json))
	writing_product_file_pointer.close()

def get_product_details(tmp_category_dir, category, reviewerID, asin, sentiment, total_reacted, helpfulness, rating, summary, date, month, year, day, title, price, related, salesRank, brand, categories):
	global global_data

	file_path_dir = tmp_category_dir + category + '/products/' + '/'.join(categories[0]) + '/'
	create_dir(file_path_dir)
	
	file_path = file_path_dir + asin + '.json'

	if asin in global_data['available_products']:
		reading_file_pointer = io.read_file(file_path)
		line = reading_file_pointer.readline()
		product_json = json.loads(line)
		reading_file_pointer.close()

		product_json['#_reviews'] += 1
		pos_senti, neg_senti = get_sentiment_scale(sentiment)
		product_json['#_+ve_reviews'] += pos_senti
		product_json['#_-ve_reviews'] += neg_senti
		product_json['total_reacted'] += total_reacted
		product_json['helpfulness'] += helpfulness
		product_json['rating'] += rating
		product_json['engaged_time'] = unixtime.days_difference(product_json['first_purchase'], [year, month, date])
		#Think about wheather its required or not
		product_json['#_products_related'] = no_products_related(related)
		product_json['buy_again'] += repeated_purchase(tmp_category_dir, category, reviewerID, asin)
		product_json['salerank'] = salesRank
		product_json['all_categories'] = categories
		#Think about wheather its required or not
		product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential'] = get_influential_details(related, asin)

		writing_product_file_pointer = io.create_file(file_path)
		io.write_line(writing_product_file_pointer, json.dumps(product_json))
		writing_product_file_pointer.close()

	else:
		global_data['available_products'][asin] = file_path
	
		product_json = {}
		product_json['#_reviews'] = 1
		product_json['#_+ve_reviews'], product_json['#_-ve_reviews'] = get_sentiment_scale(sentiment)
		product_json['total_reacted'] = total_reacted
		product_json['helpfulness'] = helpfulness
		product_json['rating'] = rating
		product_json['price'] = get_price_scale(price, category)
		product_json['first_purchase'] = [year, month, date]
		product_json['engaged_time'] = 0
		product_json['related'] = related
		product_json['#_products_related'] = no_products_related(related)
		product_json['salerank'] = salesRank
		product_json['buy_again'] = 0
		product_json['all_categories'] = categories
		product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential'] = get_influential_details(related, asin)

		writing_product_file_pointer = io.create_file(file_path)
		io.write_line(writing_product_file_pointer, json.dumps(product_json))
		writing_product_file_pointer.close()

	if asin in global_data['unavailable_products']:
		update_needed_asin = global_data['unavailable_products'][asin]
		for product in update_needed_asin:
			update_no_products_related(product)
			update_influential_details(product)
	del global_data['unavailable_products'][asin]

def get_attributes(json_line):
	FLAG = True

	if 'reviewerID' in json_line:
		reviewerID = json_line['reviewerID']
	else:
		FLAG = False
		reviewerID = ''

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

	return FLAG, reviewerID, asin, helpful, reviewText, overall, summary, unixReviewTime, title, price, related, salesRank, brand, categories

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
			index_file_pointer.close()
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
				FLAG, reviewerID, asin, helpful, reviewText, overall, summary, unixReviewTime, title, price, related, salesRank, brand, categories = get_attributes(merged_json)
				
				if FLAG:
					sentiment, total_reacted, helpfulness, rating = get_review_details(helpful, reviewText, overall)
					date, month, year, day = unix_to_attributes(unixReviewTime)
				else:
					print('Till now reject')

				line_no += 1
				same_category_flag = True
			except:
				print('🐛 ERROR:\nFILE_PATH:', file_path, '\nLINE_NO:', line_no, '\nLINE:', line)
				pass

		reading_file_pointer.close()
		print('Path no.', path_no, 'DONE:', file_path)
		path_no += 1

def create_category_dirs(tmp_category_dir, category_names):
	for category in category_names:
		io.make_dir(['reviewers', 'products', 'brands', 'categories', 'subcategories'], tmp_category_dir + category + '/')

def write_feed_data(tmp_category_dir, category_names):
	paths = synch.get_sorted_files_path(tmp_category_dir, category_names)
	print('Total number of files:', len(paths))
	create_category_dirs(tmp_category_dir, category_names)
	synch_data(paths, tmp_category_dir)

def test():
	tmp_category_dir = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/categories/'
	category_names = ['Electronics']
	write_feed_data(tmp_category_dir, category_names)

if __name__ == "__main__":
	test()