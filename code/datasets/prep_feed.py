import datasets
from utils import io, unixtime
from sentencelabel import core
import gzip
import json
import pickle
import synch
import os

#change path
price_details_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/price.pkl'
with open(price_details_path, 'rb') as f:
	price_scale = pickle.load(f)

global_data = {
				'price_scale': price_scale, 
				'available_reviewers': {},
				'unavailable_products': {},
				'available_products': {},
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

def get_brand_json(brand):
	global global_data
	reading_brand_file_pointer = io.read_file(global_data['available_brands'][brand])
	line = reading_brand_file_pointer.readline()
	brand_json = json.loads(line)
	reading_brand_file_pointer.close()
	return brand_json

def get_subcategory_json(tmp_category_dir, category, subcategory):
	global global_data
	subcategory_filepath = tmp_category_dir + category + '/subcategories/' + subcategory + '.json'
	reading_subcategory_file_pointer = io.read_file(subcategory_filepath)
	line = reading_subcategory_file_pointer.readline()
	subcategory_json = json.loads(line)
	reading_subcategory_file_pointer.close()
	return subcategory_json

def get_category_json(tmp_category_dir, category):
	global global_data
	category_filepath = tmp_category_dir + category + '/categories/' + category + '.json'
	reading_category_file_pointer = io.read_file(category_filepath)
	line = reading_category_file_pointer.readline()
	category_json = json.loads(line)
	reading_category_file_pointer.close()
	return category_json

def prepare_r_b_s_c_no_products_related(asin, also_bought, also_viewed, bought_together):
	r_b_s_c_no_related = {}
	r_b_s_c_no_related['also_bought'] = also_bought
	r_b_s_c_no_related['also_viewed'] = also_viewed
	r_b_s_c_no_related['bought_together'] = bought_together

	product_json = get_product_json(asin)
	r_b_s_c_no_related['also_bought'] -= product_json['#_products_related']['also_bought']
	r_b_s_c_no_related['also_viewed'] -= product_json['#_products_related']['also_viewed']
	r_b_s_c_no_related['bought_together'] -= product_json['#_products_related']['bought_together']
	return r_b_s_c_no_related

def r_b_s_c_no_products_related(asin, also_bought, also_viewed, bought_together):
	r_b_s_c_no_related = {}
	r_b_s_c_no_related['also_bought'] = also_bought
	r_b_s_c_no_related['also_viewed'] = also_viewed
	r_b_s_c_no_related['bought_together'] = bought_together

	product_json = get_product_json(asin)
	r_b_s_c_no_related['also_bought'] += product_json['#_products_related']['also_bought']
	r_b_s_c_no_related['also_viewed'] += product_json['#_products_related']['also_viewed']
	r_b_s_c_no_related['bought_together'] += product_json['#_products_related']['bought_together']
	return r_b_s_c_no_related

def add_review_to_reviewer(reviewer_json, asin, brand, subcategory, category):
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
	brand = {}
	subcategory = {}
	category = {}
	
	for product in reviewer_json['reviews']:
		brand[reviewer_json['reviews'][product]['brand']] = 0
		subcategory[reviewer_json['reviews'][product]['subcategory']] = 0
		category[reviewer_json['reviews'][product]['category']] = 0

	for product in reviewer_json['reviews']:
		brand[reviewer_json['reviews'][product]['brand']] += reviewer_json['reviews'][product]['#_time']
		subcategory[reviewer_json['reviews'][product]['subcategory']] += reviewer_json['reviews'][product]['#_time']
		category[reviewer_json['reviews'][product]['category']] += reviewer_json['reviews'][product]['#_time']

	return sorted(brand, key=brand.get, reverse=True)[0], sorted(subcategory, key=subcategory.get, reverse=True)[0], sorted(category, key=category.get, reverse=True)[0]

def prepare_r_b_s_c_influential_details(asin, also_bought, also_viewed, bought_together):
	influential_attributes = {}
	influential_attributes['also_bought'] = also_bought
	influential_attributes['also_viewed'] = also_viewed
	influential_attributes['bought_together'] = bought_together

	product_json = get_product_json(asin)
	mapping = {'also_bought': product_json['also_bought_influential'], 'also_viewed': product_json['also_viewed_influential'], 'bought_together': product_json['bought_together_influential']}
	for i in mapping:
		influential_attributes[i]['#_reviews'] -= mapping[i]['#_reviews']
		influential_attributes[i]['#_+ve_reviews'] -= mapping[i]['#_+ve_reviews']
		influential_attributes[i]['#_-ve_reviews'] -= mapping[i]['#_-ve_reviews']
		influential_attributes[i]['total_reacted'] -= mapping[i]['total_reacted']
		influential_attributes[i]['helpfulness'] -= mapping[i]['helpfulness']
		influential_attributes[i]['rating'] -= mapping[i]['rating']
		influential_attributes[i]['price'] -= mapping[i]['price']
		influential_attributes[i]['engaged_time'] -= mapping[i]['engaged_time']
		influential_attributes[i]['#_products_related'] -= mapping[i]['#_products_related']
		influential_attributes[i]['buy_again'] -= mapping[i]['buy_again']

	return influential_attributes['also_bought'], influential_attributes['also_viewed'], influential_attributes['bought_together']

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
		influential_attributes[i]['buy_again'] += mapping[i]['buy_again']

	return influential_attributes['also_bought'], influential_attributes['also_viewed'], influential_attributes['bought_together']

def get_reviewer_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories):
	global global_data

	subcategory = '_'.join(categories[0][1:])
	reviewer_filepath = tmp_category_dir + category + '/reviewers/' + reviewerID + '.json'

	if reviewerID in global_data['available_reviewers']:
		reading_reviewer_file_pointer = io.read_file(reviewer_filepath)
		line = reading_reviewer_file_pointer.readline()
		reading_reviewer_file_pointer.close()
		reviewer_json = json.loads(line)

		reviewer_json['#_reviews'] += 1
		reviewer_json['#_+ve_reviews'] += pos_senti
		reviewer_json['#_-ve_reviews'] += neg_senti
		reviewer_json['total_reacted'] += total_reacted
		reviewer_json['helpfulness'] += helpfulness
		reviewer_json['rating'] += rating
		reviewer_json['price'] += price_scale
		reviewer_json['engaged_time'] = unixtime.days_difference(reviewer_json['first_purchase'], [year, month, date])
		buy_again_value = buy_again(reviewer_json, asin)
		if buy_again_value == 0:
			reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'])
		reviewer_json['buy_again'] += buy_again_value
		reviewer_json['reviews'] = add_review_to_reviewer(reviewer_json, asin, brand, subcategory, category)
		reviewer_json['#_products_related'] = r_b_s_c_no_products_related(asin, reviewer_json['#_products_related']['also_bought'], reviewer_json['#_products_related']['also_viewed'], reviewer_json['#_products_related']['bought_together'])
		reviewer_json['fav_brand'], reviewer_json['fav_subcategory'], reviewer_json['fav_category'] = get_favourite(reviewer_json)
	
		writing_reviewer_file_pointer = io.create_file(reviewer_filepath)
		io.write_line(writing_reviewer_file_pointer, json.dumps(reviewer_json))
		writing_reviewer_file_pointer.close()

	else:
		global_data['available_reviewers'][reviewerID] = reviewer_filepath
		
		product_json = get_product_json(asin)
		reviewer_json = {}
		reviewer_json['#_reviews'] = 1
		reviewer_json['#_+ve_reviews'], reviewer_json['#_-ve_reviews'] = pos_senti, neg_senti
		reviewer_json['total_reacted'] = total_reacted
		reviewer_json['helpfulness'] = helpfulness
		reviewer_json['rating'] = rating
		reviewer_json['price'] = price_scale
		reviewer_json['first_purchase'] = [year, month, date]
		reviewer_json['engaged_time'] = 0
		reviewer_json['buy_again'] = 0
		reviewer_json['reviews'] = {asin: {'#_time': 1, 'category': category, 'subcategory': subcategory, 'brand': brand}}
		reviewer_json['#_products_related'] = no_products_related(related)
		reviewer_json['fav_category'] = category
		reviewer_json['fav_subcategory'] = ''
		reviewer_json['fav_brand'] = brand
		reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'] = product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential']
		
		writing_reviewer_file_pointer = io.create_file(reviewer_filepath)
		io.write_line(writing_reviewer_file_pointer, json.dumps(reviewer_json))
		writing_reviewer_file_pointer.close()

def get_brand_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories):
	global global_data

	brand_filepath = tmp_category_dir + category + '/brands/' + brand + '.json'

	if brand in global_data['available_brands']:
		reading_brand_file_pointer = io.read_file(brand_filepath)
		line = reading_brand_file_pointer.readline()
		reading_brand_file_pointer.close()
		brand_json = json.loads(line)

		brand_json['#_reviews'] += 1
		brand_json['#_+ve_reviews'] += pos_senti
		brand_json['#_-ve_reviews'] += neg_senti
		brand_json['total_reacted'] += total_reacted
		brand_json['helpfulness'] += helpfulness
		brand_json['rating'] += rating
		brand_json['price'] += price_scale
		brand_json['engaged_time'] = unixtime.days_difference(brand_json['first_purchase'], [year, month, date])
		if asin not in brand_json['products']:
			brand_json['#_products'] += 1
			brand_json['products'].append([asin])
			brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'])
			brand_json['#_products_related'] = r_b_s_c_no_products_related(asin, brand_json['#_products_related']['also_bought'], brand_json['#_products_related']['also_viewed']. brand_json['#_products_related']['bought_together'])
		
		writing_brand_file_pointer = io.create_file(brand_filepath)
		io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
		writing_brand_file_pointer.close()

	else:
		global_data['available_brands'][brand] = brand_filepath
		
		product_json = get_product_json(asin)
		brand_json = {}
		brand_json['#_reviews'] = 1
		brand_json['#_products'] = 1
		brand_json['#_+ve_reviews'], brand_json['#_-ve_reviews'] = pos_senti, neg_senti
		brand_json['total_reacted'] = total_reacted
		brand_json['helpfulness'] = helpfulness
		brand_json['rating'] = rating
		brand_json['price'] = price_scale
		brand_json['first_purchase'] = [year, month, date]
		brand_json['engaged_time'] = 0
		brand_json['products'] = [asin]
		brand_json['#_products_related'] = no_products_related(related)
		brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential']
		
		writing_brand_file_pointer = io.create_file(brand_filepath)
		io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
		writing_brand_file_pointer.close()

def get_subcategory_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories):
	global global_data

	subcategory = '_'.join(categories[0][1:])
	subcategory_filepath = tmp_category_dir + category + '/subcategories/' + subcategory + '.json'

	if subcategory in global_data['available_subcategories']:
		reading_subcategory_file_pointer = io.read_file(subcategory_filepath)
		line = reading_subcategory_file_pointer.readline()
		reading_subcategory_file_pointer.close()
		subcategory_json = json.loads(line)

		if brand not in global_data['available_subcategories'][subcategory]:
			global_data['available_subcategories'][subcategory].append(brand)
		subcategory_json['#_reviews'] += 1
		subcategory_json['#_+ve_reviews'] += pos_senti
		subcategory_json['#_-ve_reviews'] += neg_senti
		subcategory_json['total_reacted'] += total_reacted
		subcategory_json['helpfulness'] += helpfulness
		subcategory_json['rating'] += rating
		subcategory_json['price'] += price_scale
		subcategory_json['engaged_time'] = unixtime.days_difference(subcategory_json['first_purchase'], [year, month, date])
		if asin not in subcategory_json['products']:
			subcategory_json['#_products'] += 1
			subcategory_json['products'].append([asin])
			subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'])
			subcategory_json['#_products_related'] = r_b_s_c_no_products_related(asin, subcategory_json['#_products_related']['also_bought'], subcategory_json['#_products_related']['also_viewed']. subcategory_json['#_products_related']['bought_together'])
		
		writing_subcategory_file_pointer = io.create_file(subcategory_filepath)
		io.write_line(writing_subcategory_file_pointer, json.dumps(subcategory_json))
		writing_subcategory_file_pointer.close()

	else:
		global_data['available_subcategories'][subcategory] = [brand]
		
		product_json = get_product_json(asin)
		subcategory_json = {}
		subcategory_json['#_reviews'] = 1
		subcategory_json['#_products'] = 1
		subcategory_json['#_+ve_reviews'], subcategory_json['#_-ve_reviews'] = pos_senti, neg_senti
		subcategory_json['total_reacted'] = total_reacted
		subcategory_json['helpfulness'] = helpfulness
		subcategory_json['rating'] = rating
		subcategory_json['price'] = price_scale
		subcategory_json['first_purchase'] = [year, month, date]
		subcategory_json['engaged_time'] = 0
		subcategory_json['products'] = [asin]
		subcategory_json['#_products_related'] = no_products_related(related)
		subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'] = product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential']

		writing_subcategory_file_pointer = io.create_file(subcategory_filepath)
		io.write_line(writing_subcategory_file_pointer, json.dumps(subcategory_json))
		writing_subcategory_file_pointer.close()

def get_category_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories):
	global global_data

	subcategory = '_'.join(categories[0][1:])
	category_filepath = tmp_category_dir + category + '/categories/' + category + '.json'

	if category in global_data['available_categories']:
		reading_category_file_pointer = io.read_file(category_filepath)
		line = reading_category_file_pointer.readline()
		reading_category_file_pointer.close()
		category_json = json.loads(line)

		if subcategory not in global_data['available_categories'][category]:
			global_data['available_categories'][category].append(subcategory)
		category_json['#_reviews'] += 1
		category_json['#_+ve_reviews'] += pos_senti
		category_json['#_-ve_reviews'] += neg_senti
		category_json['total_reacted'] += total_reacted
		category_json['helpfulness'] += helpfulness
		category_json['rating'] += rating
		category_json['price'] += price_scale
		category_json['engaged_time'] = unixtime.days_difference(category_json['first_purchase'], [year, month, date])
		if asin not in category_json['products']:
			category_json['#_products'] += 1
			category_json['products'].append([asin])
			category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'])
			category_json['#_products_related'] = r_b_s_c_no_products_related(asin, category_json['#_products_related']['also_bought'], category_json['#_products_related']['also_viewed']. category_json['#_products_related']['bought_together'])
		
		writing_category_file_pointer = io.create_file(category_filepath)
		io.write_line(writing_category_file_pointer, json.dumps(category_json))
		writing_category_file_pointer.close()

	else:
		global_data['available_subcategories'][category] = [subcategory]
		
		product_json = get_product_json(asin)
		category_json = {}
		category_json['#_reviews'] = 1
		category_json['#_products'] = 1
		category_json['#_+ve_reviews'], category_json['#_-ve_reviews'] = pos_senti, neg_senti
		category_json['total_reacted'] = total_reacted
		category_json['helpfulness'] = helpfulness
		category_json['rating'] = rating
		category_json['price'] = price_scale
		category_json['first_purchase'] = [year, month, date]
		category_json['engaged_time'] = 0
		category_json['products'] = [asin]
		category_json['#_products_related'] = no_products_related(related)
		category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'] = product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential']

		writing_category_file_pointer = io.create_file(category_filepath)
		io.write_line(writing_category_file_pointer, json.dumps(category_json))
		writing_category_file_pointer.close()

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
		reading_file_pointer.close()

	return influential_attributes

def get_influential_details(related, asin, brand, subcategory, category):
	global global_data

	available_products = []
	for product in related['also_bought']:
		if product in global_data['available_products']:
			available_products.append(product)
		else:
			pbsc = [asin, brand, subcategory, category]
			if product in global_data['unavailable_products']:
				if pbsc not in global_data['unavailable_products'][product]:
					global_data['unavailable_products'][product].append(pbsc)
			else:
				global_data['unavailable_products'][product] = [pbsc]			
	also_bought_influential = get_influential_attributes(available_products)

	available_products = []
	for product in related['also_viewed']:
		if product in global_data['available_products']:
			available_products.append(product)
		else:
			pbsc = [asin, brand, subcategory, category]
			if product in global_data['unavailable_products']:
				if pbsc not in global_data['unavailable_products'][product]:
					global_data['unavailable_products'][product].append(pbsc)
			else:
				global_data['unavailable_products'][product] = [pbsc]
	also_viewed_influential = get_influential_attributes(available_products)

	available_products = []
	for product in related['bought_together']:
		if product in global_data['available_products']:
			available_products.append(product)
		else:
			pbsc = [asin, brand, subcategory, category]
			if product in global_data['unavailable_products']:
				if pbsc not in global_data['unavailable_products'][product]:
					global_data['unavailable_products'][product].append(pbsc)
			else:
				global_data['unavailable_products'][product] = [pbsc]
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

def update_influential_details(product, brand, subcategory, category):
	global global_data
	product_json = get_product_json(product)
	related = product_json['related']
	product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential'] = get_influential_details(related, product, brand, subcategory, category)
	writing_product_file_pointer = io.create_file(global_data['available_products'][product])
	io.write_line(writing_product_file_pointer, json.dumps(product_json))
	writing_product_file_pointer.close()

def get_product_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories):
	global global_data

	subcategory = '_'.join(categories[0][1:])
	file_path_dir = tmp_category_dir + category + '/products/' + '/'.join(categories[0]) + '/'
	create_dir(file_path_dir)
	
	file_path = file_path_dir + asin + '.json'

	if asin in global_data['available_products']:
		reading_file_pointer = io.read_file(file_path)
		line = reading_file_pointer.readline()
		product_json = json.loads(line)
		reading_file_pointer.close()

		product_json['#_reviews'] += 1
		product_json['#_+ve_reviews'] += pos_senti
		product_json['#_-ve_reviews'] += neg_senti
		product_json['total_reacted'] += total_reacted
		product_json['helpfulness'] += helpfulness
		product_json['rating'] += rating
		product_json['engaged_time'] = unixtime.days_difference(product_json['first_purchase'], [year, month, date])
		product_json['#_products_related'] = no_products_related(related)
		product_json['buy_again'] += repeated_purchase(tmp_category_dir, category, reviewerID, asin)
		product_json['all_categories'] = categories
		product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential'] = get_influential_details(related, asin, brand, subcategory, category)

		writing_product_file_pointer = io.create_file(file_path)
		io.write_line(writing_product_file_pointer, json.dumps(product_json))
		writing_product_file_pointer.close()

	else:
		global_data['available_products'][asin] = file_path
	
		product_json = {}
		product_json['#_reviews'] = 1
		product_json['#_+ve_reviews'], product_json['#_-ve_reviews'] = pos_senti, neg_senti
		product_json['total_reacted'] = total_reacted
		product_json['helpfulness'] = helpfulness
		product_json['rating'] = rating
		product_json['price'] = price_scale
		product_json['first_purchase'] = [year, month, date]
		product_json['engaged_time'] = 0
		product_json['related'] = related
		product_json['#_products_related'] = no_products_related(related)
		product_json['buy_again'] = 0
		product_json['all_categories'] = categories
		product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential'] = get_influential_details(related, asin, brand, subcategory, category)

		writing_product_file_pointer = io.create_file(file_path)
		io.write_line(writing_product_file_pointer, json.dumps(product_json))
		writing_product_file_pointer.close()

	if asin in global_data['unavailable_products']:
		update_needed = global_data['unavailable_products'][asin]

		for pbsc in update_needed:
			brand_json = get_brand_json(pbsc[1])
			also_bought_influential, also_viewed_influential, bought_together_influential = brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential']
			no_products_related = brand_json['#_products_related']
			no_products_related = prepare_r_b_s_c_no_products_related(pbsc[0], no_products_related['also_bought'], no_products_related['also_viewed'], no_products_related['bought_together'])
			also_bought_influential, also_viewed_influential, bought_together_influential = prepare_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
			brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = also_bought_influential, also_viewed_influential, bought_together_influential
			brand_json['#_products_related'] = no_products_related
			writing_brand_file_pointer = io.create_file(global_data['available_brands'][brand])
			io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
			writing_brand_file_pointer.close()

			subcategory_json = get_subcategory_json(tmp_category_dir, pbsc[3], pbsc[2])
			also_bought_influential, also_viewed_influential, bought_together_influential = subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential']
			no_products_related = subcategory_json['#_products_related']
			no_products_related = prepare_r_b_s_c_no_products_related(pbsc[0], no_products_related['also_bought'], no_products_related['also_viewed'], no_products_related['bought_together'])
			also_bought_influential, also_viewed_influential, bought_together_influential = prepare_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
			subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'] = also_bought_influential, also_viewed_influential, bought_together_influential
			subcategory_json['#_products_related'] = no_products_related
			writing_subcategory_file_pointer = io.create_file(tmp_category_dir + pbsc[3] + '/subcategories/' + pbsc[2] + '.json')
			io.write_line(writing_subcategory_file_pointer, json.dumps(subcategory_json))
			writing_subcategory_file_pointer.close()

			category_json = get_category_json(tmp_category_dir, category)
			also_bought_influential, also_viewed_influential, bought_together_influential = category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential']
			no_products_related = category_json['#_products_related']
			no_products_related = prepare_r_b_s_c_no_products_related(pbsc[0], no_products_related['also_bought'], no_products_related['also_viewed'], no_products_related['bought_together'])
			also_bought_influential, also_viewed_influential, bought_together_influential = prepare_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
			category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'] = also_bought_influential, also_viewed_influential, bought_together_influential
			category_json['#_products_related'] = no_products_related
			writing_category_file_pointer = io.create_file(tmp_category_dir + pbsc[3] + '/categories/' + pbsc[3] + '.json')
			io.write_line(writing_category_file_pointer, json.dumps(category_json))
			writing_category_file_pointer.close()

			update_no_products_related(pbsc[0])
			update_influential_details(pbsc[0], pbsc[1], pbsc[2], pbsc[3])

			brand_json = get_brand_json(pbsc[1])
			also_bought_influential, also_viewed_influential, bought_together_influential = brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential']
			no_products_related = brand_json['#_products_related']
			no_products_related = r_b_s_c_no_products_related(pbsc[0], no_products_related['also_bought'], no_products_related['also_viewed'], no_products_related['bought_together'])
			also_bought_influential, also_viewed_influential, bought_together_influential = get_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
			brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = also_bought_influential, also_viewed_influential, bought_together_influential
			brand_json['#_products_related'] = no_products_related
			writing_brand_file_pointer = io.create_file(global_data['available_brands'][brand])
			io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
			writing_brand_file_pointer.close()

			subcategory_json = get_subcategory_json(tmp_category_dir, pbsc[3], pbsc[2])
			also_bought_influential, also_viewed_influential, bought_together_influential = subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential']
			no_products_related = subcategory_json['#_products_related']
			no_products_related = r_b_s_c_no_products_related(pbsc[0], no_products_related['also_bought'], no_products_related['also_viewed'], no_products_related['bought_together'])
			also_bought_influential, also_viewed_influential, bought_together_influential = get_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
			subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'] = also_bought_influential, also_viewed_influential, bought_together_influential
			subcategory_json['#_products_related'] = no_products_related
			writing_subcategory_file_pointer = io.create_file(tmp_category_dir + pbsc[3] + '/subcategories/' + pbsc[2] + '.json')
			io.write_line(writing_subcategory_file_pointer, json.dumps(subcategory_json))
			writing_subcategory_file_pointer.close()

			category_json = get_category_json(tmp_category_dir, category)
			also_bought_influential, also_viewed_influential, bought_together_influential = category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential']
			no_products_related = category_json['#_products_related']
			no_products_related = r_b_s_c_no_products_related(pbsc[0], no_products_related['also_bought'], no_products_related['also_viewed'], no_products_related['bought_together'])
			also_bought_influential, also_viewed_influential, bought_together_influential = get_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
			category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'] = also_bought_influential, also_viewed_influential, bought_together_influential
			category_json['#_products_related'] = no_products_related
			writing_category_file_pointer = io.create_file(tmp_category_dir + pbsc[3] + '/categories/' + pbsc[3] + '.json')
			io.write_line(writing_category_file_pointer, json.dumps(category_json))
			writing_category_file_pointer.close()
		
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
			salesRank = ''	
	else:
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
					pos_senti, neg_senti = get_sentiment_scale(sentiment)
					price_scale = get_price_scale(price, category)
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
	#change path
	tmp_category_dir = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/categories/'
	category_names = ['Electronics']
	write_feed_data(tmp_category_dir, category_names)

if __name__ == "__main__":
	test()