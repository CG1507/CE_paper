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
	price_scale_dict = pickle.load(f)

global_data = {
				'price_scale': price_scale_dict, 
				'available_products': {},
				'available_brands': {},
				'available_subcategories': {},
				'available_categories': {},
				'available_reviewers': {},
				'unavailable_products': {}
			  }

def save_global_data():
	global global_data
	global_data_file_pointer = open('./global_data.pkl', 'wb')
	pickle.dump(global_data, global_data_file_pointer)
	global_data_file_pointer.close()

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

def get_reviewer_json(reviewerID):
	global global_data
	reading_reviewer_file_pointer = io.read_file(global_data['available_reviewers'][reviewerID])
	line = reading_reviewer_file_pointer.readline()
	reviewer_json = json.loads(line)
	reading_reviewer_file_pointer.close()
	return reviewer_json

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
		reviewer_json['reviews'][asin] = {}
		reviewer_json['reviews'][asin]['#_time'] = 1
		reviewer_json['reviews'][asin]['brand'] = brand
		reviewer_json['reviews'][asin]['subcategory'] = subcategory
		reviewer_json['reviews'][asin]['category'] = category
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
		influential_attributes[i]['total_time'] -= mapping[i]['total_time']
		influential_attributes[i]['buy_again'] -= mapping[i]['buy_again']
		influential_attributes[i]['#_products_related']['also_bought'] -= mapping[i]['#_products_related']['also_bought']
		influential_attributes[i]['#_products_related']['also_viewed'] -= mapping[i]['#_products_related']['also_viewed']
		influential_attributes[i]['#_products_related']['bought_together'] -= mapping[i]['#_products_related']['bought_together']

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
		influential_attributes[i]['total_time'] += mapping[i]['total_time']
		influential_attributes[i]['buy_again'] += mapping[i]['buy_again']
		influential_attributes[i]['#_products_related']['also_bought'] += mapping[i]['#_products_related']['also_bought']
		influential_attributes[i]['#_products_related']['also_viewed'] += mapping[i]['#_products_related']['also_viewed']
		influential_attributes[i]['#_products_related']['bought_together'] += mapping[i]['#_products_related']['bought_together']

	return influential_attributes['also_bought'], influential_attributes['also_viewed'], influential_attributes['bought_together']

def get_related_match(related, products, fav_brand, fav_subcategory, fav_category):
	global global_data
	related_fav = {}
	related_fav['bought'] = 0
	related_fav['brand'] = 0
	related_fav['subcategory'] = 0
	related_fav['category'] = 0
	new_related = []
	for r in related['also_viewed']:
		if r in global_data['available_products']:
			new_related.append(r)

	for product in new_related:
		if product in products:
			related_fav['bought'] += 1

		product_json = get_product_json(product)
		related_product_brand = product_json['brand']
		related_product_subcategory = product_json['subcategory']
		related_product_category = product_json['category']
		if fav_brand == related_product_brand:
			related_fav['brand'] += 1

		if fav_subcategory == related_product_subcategory:
			related_fav['subcategory'] += 1

		if fav_category == related_product_category:
			related_fav['category'] += 1

	return related_fav['bought'], related_fav['brand'], related_fav['subcategory'], related_fav['category']

def get_reviewer_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories):
	global global_data

	subcategory = '_'.join(categories[0][1:])
	if '/' in subcategory:
		subcategory = subcategory.replace('/', '')
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
		reviewer_json['total_time'] = unixtime.days_difference(reviewer_json['first_purchase'], [year, month, date])
		reviewer_json['engaged_time'] = reviewer_json['total_time'] - reviewer_json['engaged_time']
		
		buy_again_value = buy_again(reviewer_json, asin)
		reviewer_json['buy_again'] += buy_again_value
		reviewer_json['#_products'] += (1 - buy_again_value)

		reviewer_json['reviews'] = add_review_to_reviewer(reviewer_json, asin, brand, subcategory, category)
		reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'])
		reviewer_json['#_products_related'] = r_b_s_c_no_products_related(asin, reviewer_json['#_products_related']['also_bought'], reviewer_json['#_products_related']['also_viewed'], reviewer_json['#_products_related']['bought_together'])
		
		reviewer_json['curr_product_brand'] = brand
		reviewer_json['curr_product_subcategory'] = subcategory
		reviewer_json['curr_product_category'] = category
		reviewer_json['fav_brand_name'], reviewer_json['fav_subcategory_name'], reviewer_json['fav_category_name'] = get_favourite(reviewer_json)
		reviewer_json['#_related_bought'], reviewer_json['#_related_brands_fav'], reviewer_json['#_related_subcategory_fav'], reviewer_json['#_related_category_fav'] = get_related_match(related, reviewer_json['reviews'].keys(), reviewer_json['fav_brand_name'], reviewer_json['fav_subcategory_name'], reviewer_json['fav_category_name'])
		
		if reviewer_json['fav_brand'] == reviewer_json['curr_product_brand']:
			reviewer_json['fav_brand'] = 1
		else:
			reviewer_json['fav_brand'] = 0

		if reviewer_json['fav_subcategory'] == reviewer_json['curr_product_subcategory']:
			reviewer_json['fav_subcategory'] = 1
		else:
			reviewer_json['fav_subcategory'] = 0

		if reviewer_json['fav_category'] == reviewer_json['curr_product_category']:
			reviewer_json['fav_category'] = 1
		else:
			reviewer_json['fav_category'] = 0


		writing_reviewer_file_pointer = io.create_file(reviewer_filepath)
		io.write_line(writing_reviewer_file_pointer, json.dumps(reviewer_json))
		writing_reviewer_file_pointer.close()

	else:
		global_data['available_reviewers'][reviewerID] = reviewer_filepath
		
		product_json = get_product_json(asin)
		reviewer_json = {}
		reviewer_json['#_reviews'] = 1
		reviewer_json['#_products'] = 1
		reviewer_json['#_+ve_reviews'], reviewer_json['#_-ve_reviews'] = pos_senti, neg_senti
		reviewer_json['total_reacted'] = total_reacted
		reviewer_json['helpfulness'] = helpfulness
		reviewer_json['rating'] = rating
		reviewer_json['price'] = price_scale
		reviewer_json['first_purchase'] = [year, month, date]
		reviewer_json['total_time'] = 0
		reviewer_json['engaged_time'] = 0
		reviewer_json['buy_again'] = 0
		reviewer_json['reviews'] = {asin: {'#_time': 1, 'category': category, 'subcategory': subcategory, 'brand': brand}}
		reviewer_json['fav_brand_name'] = brand
		reviewer_json['fav_subcategory_name'] = subcategory
		reviewer_json['fav_category_name'] = category
		reviewer_json['#_related_bought'], reviewer_json['#_related_brands_fav'], reviewer_json['#_related_subcategory_fav'], reviewer_json['#_related_category_fav'] = get_related_match(related, [asin], reviewer_json['fav_brand_name'], reviewer_json['fav_subcategory_name'], reviewer_json['fav_category_name'])
		reviewer_json['fav_brand'] = 1
		reviewer_json['fav_subcategory'] = 1
		reviewer_json['fav_category'] = 1
		reviewer_json['#_products_related'] = product_json['#_products_related']
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
		brand_json['total_time'] = unixtime.days_difference(brand_json['first_purchase'], [year, month, date])
		brand_json['engaged_time'] = brand_json['total_time'] - brand_json['engaged_time']
		if reviewerID in global_data['available_reviewers']:
			repeated_purchase_value = repeated_purchase(tmp_category_dir, category, reviewerID, asin)
		else:
			repeated_purchase_value = 0
		brand_json['buy_again'] += repeated_purchase_value
		if asin not in brand_json['products']:
			brand_json['#_products'] += 1
			brand_json['products'].append(asin)
			brand_json['price'] += price_scale

		brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'])
		brand_json['#_products_related'] = r_b_s_c_no_products_related(asin, brand_json['#_products_related']['also_bought'], brand_json['#_products_related']['also_viewed'], brand_json['#_products_related']['bought_together'])
	
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
		brand_json['total_time'] = 0
		brand_json['engaged_time'] = 0
		brand_json['products'] = [asin]
		brand_json['buy_again'] = 0
		brand_json['#_products_related'] = product_json['#_products_related']
		brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential']
		
		writing_brand_file_pointer = io.create_file(brand_filepath)
		io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
		writing_brand_file_pointer.close()

def get_subcategory_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories):
	global global_data

	subcategory = '_'.join(categories[0][1:])
	if '/' in subcategory:
		subcategory = subcategory.replace('/', '')
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
		subcategory_json['total_time'] = unixtime.days_difference(subcategory_json['first_purchase'], [year, month, date])
		subcategory_json['engaged_time'] = subcategory_json['total_time'] - subcategory_json['engaged_time']
		if reviewerID in global_data['available_reviewers']:
			repeated_purchase_value = repeated_purchase(tmp_category_dir, category, reviewerID, asin)
		else:
			repeated_purchase_value = 0
		subcategory_json['buy_again'] += repeated_purchase_value
		if asin not in subcategory_json['products']:
			subcategory_json['#_products'] += 1
			subcategory_json['products'].append(asin)
			subcategory_json['price'] += price_scale
	
		subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'])
		subcategory_json['#_products_related'] = r_b_s_c_no_products_related(asin, subcategory_json['#_products_related']['also_bought'], subcategory_json['#_products_related']['also_viewed'], subcategory_json['#_products_related']['bought_together'])
			
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
		subcategory_json['total_time'] = 0
		subcategory_json['engaged_time'] = 0
		subcategory_json['products'] = [asin]
		subcategory_json['buy_again'] = 0
		subcategory_json['#_products_related'] = product_json['#_products_related']
		subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'] = product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential']

		writing_subcategory_file_pointer = io.create_file(subcategory_filepath)
		io.write_line(writing_subcategory_file_pointer, json.dumps(subcategory_json))
		writing_subcategory_file_pointer.close()

def get_category_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories):
	global global_data

	subcategory = '_'.join(categories[0][1:])
	if '/' in subcategory:
		subcategory = subcategory.replace('/', '')
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
		category_json['total_time'] = unixtime.days_difference(category_json['first_purchase'], [year, month, date])
		category_json['engaged_time'] = category_json['total_time'] - category_json['engaged_time']
		if reviewerID in global_data['available_reviewers']:
			repeated_purchase_value = repeated_purchase(tmp_category_dir, category, reviewerID, asin)
		else:
			repeated_purchase_value = 0
		category_json['buy_again'] += repeated_purchase_value
		if asin not in category_json['products']:
			category_json['#_products'] += 1
			category_json['products'].append(asin)
			category_json['price'] += price_scale		

		category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'] = get_r_b_s_c_influential_details(asin, category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'])
		category_json['#_products_related'] = r_b_s_c_no_products_related(asin, category_json['#_products_related']['also_bought'], category_json['#_products_related']['also_viewed'], category_json['#_products_related']['bought_together'])
	
		writing_category_file_pointer = io.create_file(category_filepath)
		io.write_line(writing_category_file_pointer, json.dumps(category_json))
		writing_category_file_pointer.close()

	else:
		global_data['available_categories'][category] = [subcategory]
		
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
		category_json['total_time'] = 0
		category_json['engaged_time'] = 0
		category_json['buy_again'] = 0
		category_json['products'] = [asin]
		category_json['#_products_related'] = product_json['#_products_related']
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
	date = unixtime.get_date(date_time)
	month = unixtime.get_month(date_time)
	year = unixtime.get_year(date_time)
	day = unixtime.get_day(date_time)

	return int(date), int(month), int(year), int(day)

def no_products_related(related):
	global global_data
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
	reading_reviewer_file_pointer = io.read_file(tmp_category_dir + category + '/reviewers/' + reviewerID + '.json')
	line = reading_reviewer_file_pointer.readline()
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
	influential_attributes['total_time'] = 0
	influential_attributes['buy_again'] = 0
	influential_attributes['#_products_related'] = {}
	influential_attributes['#_products_related']['also_bought'] = 0
	influential_attributes['#_products_related']['also_viewed'] = 0
	influential_attributes['#_products_related']['bought_together'] = 0

	for product in available_products:
		reading_file_pointer = io.read_file(global_data['available_products'][product])
		line = reading_file_pointer.readline()
		product_json = json.loads(line)
		reading_file_pointer.close()

		influential_attributes['#_reviews'] += product_json['#_reviews']
		influential_attributes['#_+ve_reviews'] += product_json['#_+ve_reviews']
		influential_attributes['#_-ve_reviews'] += product_json['#_-ve_reviews']
		influential_attributes['total_reacted'] += product_json['total_reacted']
		influential_attributes['helpfulness'] += product_json['helpfulness']
		influential_attributes['rating'] += product_json['rating']
		influential_attributes['price'] += product_json['price']
		influential_attributes['engaged_time'] += product_json['engaged_time']
		influential_attributes['total_time'] += product_json['total_time']
		influential_attributes['buy_again'] += product_json['buy_again']
		influential_attributes['#_products_related']['also_bought'] += product_json['#_products_related']['also_bought']
		influential_attributes['#_products_related']['also_viewed'] += product_json['#_products_related']['also_viewed']
		influential_attributes['#_products_related']['bought_together'] += product_json['#_products_related']['bought_together']

	return influential_attributes

def get_influential_details(related, asin, brand, subcategory, category, reviewerID):
	global global_data

	available_products = []
	for product in related['also_bought']:
		if product in global_data['available_products']:
			available_products.append(product)
		else:
			pbsc = [asin, brand, subcategory, category, reviewerID]
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
			pbsc = [asin, brand, subcategory, category, reviewerID]
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
			pbsc = [asin, brand, subcategory, category, reviewerID]
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

def update_influential_details(product, brand, subcategory, category, reviewerID):
	global global_data
	product_json = get_product_json(product)
	related = product_json['related']
	product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential'] = get_influential_details(related, product, brand, subcategory, category, reviewerID)
	writing_product_file_pointer = io.create_file(global_data['available_products'][product])
	io.write_line(writing_product_file_pointer, json.dumps(product_json))
	writing_product_file_pointer.close()

def get_product_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories):
	global global_data

	subcategory = '_'.join(categories[0][1:])
	if '/' in subcategory:
		subcategory = subcategory.replace('/', '')
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
		product_json['total_time'] = unixtime.days_difference(product_json['first_purchase'], [year, month, date])
		product_json['engaged_time'] = product_json['total_time'] - product_json['engaged_time']
		
		if reviewerID in global_data['available_reviewers']:
			repeated_purchase_value = repeated_purchase(tmp_category_dir, category, reviewerID, asin)
		else:
			repeated_purchase_value = 0
		product_json['buy_again'] += repeated_purchase_value

		brand_json = get_brand_json(brand)
		also_bought_influential, also_viewed_influential, bought_together_influential = brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential']
		no_products_related_dict = brand_json['#_products_related']
		brand_json['#_products_related'] = prepare_r_b_s_c_no_products_related(asin, no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
		brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = prepare_r_b_s_c_influential_details(asin, also_bought_influential, also_viewed_influential, bought_together_influential)
		writing_brand_file_pointer = io.create_file(global_data['available_brands'][brand])
		io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
		writing_brand_file_pointer.close()

		subcategory_json = get_subcategory_json(tmp_category_dir, category, subcategory)
		also_bought_influential, also_viewed_influential, bought_together_influential = subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential']
		no_products_related_dict = subcategory_json['#_products_related']
		subcategory_json['#_products_related'] = prepare_r_b_s_c_no_products_related(asin, no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
		subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'] = prepare_r_b_s_c_influential_details(asin, also_bought_influential, also_viewed_influential, bought_together_influential)
		writing_subcategory_file_pointer = io.create_file(tmp_category_dir + category + '/subcategories/' + subcategory + '.json')
		io.write_line(writing_subcategory_file_pointer, json.dumps(subcategory_json))
		writing_subcategory_file_pointer.close()

		category_json = get_category_json(tmp_category_dir, category)
		also_bought_influential, also_viewed_influential, bought_together_influential = category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential']
		no_products_related_dict = category_json['#_products_related']
		category_json['#_products_related'] = prepare_r_b_s_c_no_products_related(asin, no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
		category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'] = prepare_r_b_s_c_influential_details(asin, also_bought_influential, also_viewed_influential, bought_together_influential)
		writing_category_file_pointer = io.create_file(tmp_category_dir + category + '/categories/' + category + '.json')
		io.write_line(writing_category_file_pointer, json.dumps(category_json))
		writing_category_file_pointer.close()

		if repeated_purchase_value:
			reviewer_json = get_reviewer_json(reviewerID)
			also_bought_influential, also_viewed_influential, bought_together_influential = reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential']
			no_products_related_dict = reviewer_json['#_products_related']
			reviewer_json['#_products_related'] = prepare_r_b_s_c_no_products_related(asin, no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
			reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'] = prepare_r_b_s_c_influential_details(asin, also_bought_influential, also_viewed_influential, bought_together_influential)
			writing_reviewer_file_pointer = io.create_file(global_data['available_reviewers'][reviewerID])
			io.write_line(writing_reviewer_file_pointer, json.dumps(reviewer_json))
			writing_reviewer_file_pointer.close()

		product_json['#_products_related'] = no_products_related(related)
		product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential'] = get_influential_details(related, asin, brand, subcategory, category, reviewerID)

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
		product_json['total_time'] = 0
		product_json['engaged_time'] = 0
		product_json['brand'] = brand
		product_json['subcategory'] = subcategory
		product_json['category'] = category
		product_json['related'] = related
		product_json['buy_again'] = 0
		product_json['#_products_related'] = no_products_related(related)
		product_json['also_bought_influential'], product_json['also_viewed_influential'], product_json['bought_together_influential'] = get_influential_details(related, asin, brand, subcategory, category, reviewerID)

		writing_product_file_pointer = io.create_file(file_path)
		io.write_line(writing_product_file_pointer, json.dumps(product_json))
		writing_product_file_pointer.close()

		if asin in global_data['unavailable_products']:
			update_needed = global_data['unavailable_products'][asin]

			for pbsc in update_needed:
				brand_json = get_brand_json(pbsc[1])
				also_bought_influential, also_viewed_influential, bought_together_influential = brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential']
				no_products_related_dict = brand_json['#_products_related']
				brand_json['#_products_related'] = prepare_r_b_s_c_no_products_related(pbsc[0], no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
				brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = prepare_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
				writing_brand_file_pointer = io.create_file(global_data['available_brands'][pbsc[1]])
				io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
				writing_brand_file_pointer.close()

				subcategory_json = get_subcategory_json(tmp_category_dir, pbsc[3], pbsc[2])
				also_bought_influential, also_viewed_influential, bought_together_influential = subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential']
				no_products_related_dict = subcategory_json['#_products_related']
				subcategory_json['#_products_related'] = prepare_r_b_s_c_no_products_related(pbsc[0], no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
				subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'] = prepare_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
				writing_subcategory_file_pointer = io.create_file(tmp_category_dir + pbsc[3] + '/subcategories/' + pbsc[2] + '.json')
				io.write_line(writing_subcategory_file_pointer, json.dumps(subcategory_json))
				writing_subcategory_file_pointer.close()

				category_json = get_category_json(tmp_category_dir, pbsc[3])
				also_bought_influential, also_viewed_influential, bought_together_influential = category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential']
				no_products_related_dict = category_json['#_products_related']
				category_json['#_products_related'] = prepare_r_b_s_c_no_products_related(pbsc[0], no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
				category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'] = prepare_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
				writing_category_file_pointer = io.create_file(tmp_category_dir + pbsc[3] + '/categories/' + pbsc[3] + '.json')
				io.write_line(writing_category_file_pointer, json.dumps(category_json))
				writing_category_file_pointer.close()

				reviewer_json = get_reviewer_json(pbsc[4])
				also_bought_influential, also_viewed_influential, bought_together_influential = reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential']
				no_products_related_dict = reviewer_json['#_products_related']
				reviewer_json['#_products_related'] = prepare_r_b_s_c_no_products_related(pbsc[0], no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
				reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'] = prepare_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
				writing_reviewer_file_pointer = io.create_file(global_data['available_reviewers'][pbsc[4]])
				io.write_line(writing_reviewer_file_pointer, json.dumps(reviewer_json))
				writing_reviewer_file_pointer.close()

				update_no_products_related(pbsc[0])
				update_influential_details(pbsc[0], pbsc[1], pbsc[2], pbsc[3], pbsc[4])

				brand_json = get_brand_json(pbsc[1])
				also_bought_influential, also_viewed_influential, bought_together_influential = brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential']
				no_products_related_dict = brand_json['#_products_related']
				brand_json['#_products_related'] = r_b_s_c_no_products_related(pbsc[0], no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
				brand_json['also_bought_influential'], brand_json['also_viewed_influential'], brand_json['bought_together_influential'] = get_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
				writing_brand_file_pointer = io.create_file(global_data['available_brands'][pbsc[1]])
				io.write_line(writing_brand_file_pointer, json.dumps(brand_json))
				writing_brand_file_pointer.close()

				subcategory_json = get_subcategory_json(tmp_category_dir, pbsc[3], pbsc[2])
				also_bought_influential, also_viewed_influential, bought_together_influential = subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential']
				no_products_related_dict = subcategory_json['#_products_related']
				subcategory_json['#_products_related'] = r_b_s_c_no_products_related(pbsc[0], no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
				subcategory_json['also_bought_influential'], subcategory_json['also_viewed_influential'], subcategory_json['bought_together_influential'] = get_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
				writing_subcategory_file_pointer = io.create_file(tmp_category_dir + pbsc[3] + '/subcategories/' + pbsc[2] + '.json')
				io.write_line(writing_subcategory_file_pointer, json.dumps(subcategory_json))
				writing_subcategory_file_pointer.close()

				category_json = get_category_json(tmp_category_dir, pbsc[3])
				also_bought_influential, also_viewed_influential, bought_together_influential = category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential']
				no_products_related_dict = category_json['#_products_related']
				category_json['#_products_related'] = r_b_s_c_no_products_related(pbsc[0], no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
				category_json['also_bought_influential'], category_json['also_viewed_influential'], category_json['bought_together_influential'] = get_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
				writing_category_file_pointer = io.create_file(tmp_category_dir + pbsc[3] + '/categories/' + pbsc[3] + '.json')
				io.write_line(writing_category_file_pointer, json.dumps(category_json))
				writing_category_file_pointer.close()
			
				reviewer_json = get_reviewer_json(pbsc[4])
				also_bought_influential, also_viewed_influential, bought_together_influential = reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential']
				no_products_related_dict = reviewer_json['#_products_related']
				reviewer_json['#_products_related'] = r_b_s_c_no_products_related(pbsc[0], no_products_related_dict['also_bought'], no_products_related_dict['also_viewed'], no_products_related_dict['bought_together'])
				reviewer_json['also_bought_influential'], reviewer_json['also_viewed_influential'], reviewer_json['bought_together_influential'] = get_r_b_s_c_influential_details(pbsc[0], also_bought_influential, also_viewed_influential, bought_together_influential)
				writing_reviewer_file_pointer = io.create_file(global_data['available_reviewers'][pbsc[4]])
				io.write_line(writing_reviewer_file_pointer, json.dumps(reviewer_json))
				writing_reviewer_file_pointer.close()

			del global_data['unavailable_products'][asin]

def get_attributes(json_line):
	FLAG = True
	
	if 'reviewerID' in json_line:
		reviewerID = str(json_line['reviewerID'])
	else:
		FLAG = False
		reviewerID = ''
	
	if 'asin' in json_line:
		asin = str(json_line['asin'])
	else:
		asin = ''

	if 'helpful' in json_line:
		helpful = json_line['helpful']
	else:
		helpful = [0, 0]

	if 'reviewText'	in json_line:
		reviewText = str(json_line['reviewText'])
	else:
		reviewText = ''

	if 'overall' in json_line:
		overall = float(json_line['overall'])
	else:
		overall = 0.0

	if 'summary' in json_line:
		summary = json_line['summary']
	else:
		summary = ''

	if 'unixReviewTime' in json_line:
		unixReviewTime = str(json_line['unixReviewTime'])
	else:
		unixReviewTime = ''

	if 'title' in json_line:
		title = json_line['title']
	else:
		title = ''

	if 'price' in json_line:
		price = float(json_line['price'])
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
		salesRank = json_line['salesRank']
	else:
		salesRank = ''

	if 'brand' in json_line:
		brand = str(json_line['brand'])
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

def get_json(tmp_category_dir, asin, brand, subcategory, category, reviewerID):
	product_json = get_product_json(asin)
	brand_json = get_brand_json(brand)
	subcategory_json = get_subcategory_json(tmp_category_dir, category, subcategory)
	category_json = get_category_json(tmp_category_dir, category)
	reviewer_json = get_reviewer_json(reviewerID)
	return product_json, brand_json, subcategory_json, category_json, reviewer_json

def write_csv(tmp_category_dir, csv_file_path, product_json, brand_json, subcategory_json, category_json, reviewer_json, total_reacted, helpfulness, rating, date, month, year, day, price_scale, sentiment):
	csv_file_pointer = io.append_file(csv_file_path)
	csv_list = []

	csv_list.append(str(sentiment))
	if total_reacted == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(helpfulness/total_reacted))
	csv_list.append(str(rating))
	csv_list.append(str(date))
	csv_list.append(str(month))
	csv_list.append(str(year))
	csv_list.append(str(day))
	csv_list.append(str(price_scale))

	csv_list.append(str(product_json['#_reviews']))
	csv_list.append(str(product_json['#_+ve_reviews']))
	csv_list.append(str(product_json['#_-ve_reviews']))
	if product_json['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(product_json['helpfulness']/product_json['total_reacted']))
	csv_list.append(str(product_json['rating']))
	csv_list.append(str(product_json['price']))
	csv_list.append(str(product_json['total_time']))
	csv_list.append(str(product_json['engaged_time']))
	csv_list.append(str(product_json['buy_again']))
	csv_list.append(str(product_json['#_products_related']['also_bought']))
	csv_list.append(str(product_json['#_products_related']['also_viewed']))
	csv_list.append(str(product_json['#_products_related']['bought_together']))

	csv_list.append(str(product_json['also_bought_influential']['#_reviews']))
	csv_list.append(str(product_json['also_bought_influential']['#_+ve_reviews']))
	csv_list.append(str(product_json['also_bought_influential']['#_-ve_reviews']))
	if product_json['also_bought_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(product_json['also_bought_influential']['helpfulness'] / product_json['also_bought_influential']['total_reacted']))
	csv_list.append(str(product_json['also_bought_influential']['rating']))
	csv_list.append(str(product_json['also_bought_influential']['price']))
	csv_list.append(str(product_json['also_bought_influential']['engaged_time']))
	csv_list.append(str(product_json['also_bought_influential']['buy_again']))
	csv_list.append(str(product_json['also_bought_influential']['#_products_related']['also_bought']))
	csv_list.append(str(product_json['also_bought_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(product_json['also_bought_influential']['#_products_related']['bought_together']))

	csv_list.append(str(product_json['also_viewed_influential']['#_reviews']))
	csv_list.append(str(product_json['also_viewed_influential']['#_+ve_reviews']))
	csv_list.append(str(product_json['also_viewed_influential']['#_-ve_reviews']))
	if product_json['also_viewed_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(product_json['also_viewed_influential']['helpfulness'] / product_json['also_viewed_influential']['total_reacted']))
	csv_list.append(str(product_json['also_viewed_influential']['rating']))
	csv_list.append(str(product_json['also_viewed_influential']['price']))
	csv_list.append(str(product_json['also_viewed_influential']['engaged_time']))
	csv_list.append(str(product_json['also_viewed_influential']['buy_again']))
	csv_list.append(str(product_json['also_viewed_influential']['#_products_related']['also_bought']))
	csv_list.append(str(product_json['also_viewed_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(product_json['also_viewed_influential']['#_products_related']['bought_together']))

	csv_list.append(str(product_json['bought_together_influential']['#_reviews']))
	csv_list.append(str(product_json['bought_together_influential']['#_+ve_reviews']))
	csv_list.append(str(product_json['bought_together_influential']['#_-ve_reviews']))
	if product_json['bought_together_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(product_json['bought_together_influential']['helpfulness'] / product_json['bought_together_influential']['total_reacted']))
	csv_list.append(str(product_json['bought_together_influential']['rating']))
	csv_list.append(str(product_json['bought_together_influential']['price']))
	csv_list.append(str(product_json['bought_together_influential']['engaged_time']))
	csv_list.append(str(product_json['bought_together_influential']['buy_again']))
	csv_list.append(str(product_json['bought_together_influential']['#_products_related']['also_bought']))
	csv_list.append(str(product_json['bought_together_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(product_json['bought_together_influential']['#_products_related']['bought_together']))

	csv_list.append(str(brand_json['#_reviews']))
	csv_list.append(str(brand_json['#_products']))
	csv_list.append(str(brand_json['#_+ve_reviews']))
	csv_list.append(str(brand_json['#_-ve_reviews']))
	if brand_json['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(brand_json['helpfulness'] / brand_json['total_reacted']))
	csv_list.append(str(brand_json['rating']))
	csv_list.append(str(brand_json['price']))
	csv_list.append(str(brand_json['total_time']))
	csv_list.append(str(brand_json['engaged_time']))
	csv_list.append(str(brand_json['buy_again']))
	csv_list.append(str(brand_json['#_products_related']['also_bought']))
	csv_list.append(str(brand_json['#_products_related']['also_viewed']))
	csv_list.append(str(brand_json['#_products_related']['bought_together']))

	csv_list.append(str(brand_json['also_bought_influential']['#_reviews']))
	csv_list.append(str(brand_json['also_bought_influential']['#_+ve_reviews']))
	csv_list.append(str(brand_json['also_bought_influential']['#_-ve_reviews']))
	if brand_json['also_bought_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(brand_json['also_bought_influential']['helpfulness'] / brand_json['also_bought_influential']['total_reacted']))
	csv_list.append(str(brand_json['also_bought_influential']['rating']))
	csv_list.append(str(brand_json['also_bought_influential']['price']))
	csv_list.append(str(brand_json['also_bought_influential']['engaged_time']))
	csv_list.append(str(brand_json['also_bought_influential']['buy_again']))
	csv_list.append(str(brand_json['also_bought_influential']['#_products_related']['also_bought']))
	csv_list.append(str(brand_json['also_bought_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(brand_json['also_bought_influential']['#_products_related']['bought_together']))

	csv_list.append(str(brand_json['also_viewed_influential']['#_reviews']))
	csv_list.append(str(brand_json['also_viewed_influential']['#_+ve_reviews']))
	csv_list.append(str(brand_json['also_viewed_influential']['#_-ve_reviews']))
	if brand_json['also_viewed_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(brand_json['also_viewed_influential']['helpfulness'] / brand_json['also_viewed_influential']['total_reacted']))
	csv_list.append(str(brand_json['also_viewed_influential']['rating']))
	csv_list.append(str(brand_json['also_viewed_influential']['price']))
	csv_list.append(str(brand_json['also_viewed_influential']['engaged_time']))
	csv_list.append(str(brand_json['also_viewed_influential']['buy_again']))
	csv_list.append(str(brand_json['also_viewed_influential']['#_products_related']['also_bought']))
	csv_list.append(str(brand_json['also_viewed_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(brand_json['also_viewed_influential']['#_products_related']['bought_together']))

	csv_list.append(str(brand_json['bought_together_influential']['#_reviews']))
	csv_list.append(str(brand_json['bought_together_influential']['#_+ve_reviews']))
	csv_list.append(str(brand_json['bought_together_influential']['#_-ve_reviews']))
	if brand_json['bought_together_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(brand_json['bought_together_influential']['helpfulness'] / brand_json['bought_together_influential']['total_reacted']))
	csv_list.append(str(brand_json['bought_together_influential']['rating']))
	csv_list.append(str(brand_json['bought_together_influential']['price']))
	csv_list.append(str(brand_json['bought_together_influential']['engaged_time']))
	csv_list.append(str(brand_json['bought_together_influential']['buy_again']))
	csv_list.append(str(brand_json['bought_together_influential']['#_products_related']['also_bought']))
	csv_list.append(str(brand_json['bought_together_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(brand_json['bought_together_influential']['#_products_related']['bought_together']))

	csv_list.append(str(subcategory_json['#_reviews']))
	csv_list.append(str(subcategory_json['#_products']))
	csv_list.append(str(subcategory_json['#_+ve_reviews']))
	csv_list.append(str(subcategory_json['#_-ve_reviews']))
	if subcategory_json['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(subcategory_json['helpfulness'] / subcategory_json['total_reacted']))
	csv_list.append(str(subcategory_json['rating']))
	csv_list.append(str(subcategory_json['price']))
	csv_list.append(str(subcategory_json['total_time']))
	csv_list.append(str(subcategory_json['engaged_time']))
	csv_list.append(str(subcategory_json['buy_again']))
	csv_list.append(str(subcategory_json['#_products_related']['also_bought']))
	csv_list.append(str(subcategory_json['#_products_related']['also_viewed']))
	csv_list.append(str(subcategory_json['#_products_related']['bought_together']))

	csv_list.append(str(subcategory_json['also_bought_influential']['#_reviews']))
	csv_list.append(str(subcategory_json['also_bought_influential']['#_+ve_reviews']))
	csv_list.append(str(subcategory_json['also_bought_influential']['#_-ve_reviews']))
	if subcategory_json['also_bought_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(subcategory_json['also_bought_influential']['helpfulness'] / subcategory_json['also_bought_influential']['total_reacted']))
	csv_list.append(str(subcategory_json['also_bought_influential']['rating']))
	csv_list.append(str(subcategory_json['also_bought_influential']['price']))
	csv_list.append(str(subcategory_json['also_bought_influential']['engaged_time']))
	csv_list.append(str(subcategory_json['also_bought_influential']['buy_again']))
	csv_list.append(str(subcategory_json['also_bought_influential']['#_products_related']['also_bought']))
	csv_list.append(str(subcategory_json['also_bought_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(subcategory_json['also_bought_influential']['#_products_related']['bought_together']))

	csv_list.append(str(subcategory_json['also_viewed_influential']['#_reviews']))
	csv_list.append(str(subcategory_json['also_viewed_influential']['#_+ve_reviews']))
	csv_list.append(str(subcategory_json['also_viewed_influential']['#_-ve_reviews']))
	if subcategory_json['also_viewed_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(subcategory_json['also_viewed_influential']['helpfulness'] / subcategory_json['also_viewed_influential']['total_reacted']))
	csv_list.append(str(subcategory_json['also_viewed_influential']['rating']))
	csv_list.append(str(subcategory_json['also_viewed_influential']['price']))
	csv_list.append(str(subcategory_json['also_viewed_influential']['engaged_time']))
	csv_list.append(str(subcategory_json['also_viewed_influential']['buy_again']))
	csv_list.append(str(subcategory_json['also_viewed_influential']['#_products_related']['also_bought']))
	csv_list.append(str(subcategory_json['also_viewed_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(subcategory_json['also_viewed_influential']['#_products_related']['bought_together']))

	csv_list.append(str(subcategory_json['bought_together_influential']['#_reviews']))
	csv_list.append(str(subcategory_json['bought_together_influential']['#_+ve_reviews']))
	csv_list.append(str(subcategory_json['bought_together_influential']['#_-ve_reviews']))
	if subcategory_json['bought_together_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:	
		csv_list.append(str(subcategory_json['bought_together_influential']['helpfulness'] / subcategory_json['bought_together_influential']['total_reacted']))
	csv_list.append(str(subcategory_json['bought_together_influential']['rating']))
	csv_list.append(str(subcategory_json['bought_together_influential']['price']))
	csv_list.append(str(subcategory_json['bought_together_influential']['engaged_time']))
	csv_list.append(str(subcategory_json['bought_together_influential']['buy_again']))
	csv_list.append(str(subcategory_json['bought_together_influential']['#_products_related']['also_bought']))
	csv_list.append(str(subcategory_json['bought_together_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(subcategory_json['bought_together_influential']['#_products_related']['bought_together']))

	csv_list.append(str(category_json['#_reviews']))
	csv_list.append(str(category_json['#_products']))
	csv_list.append(str(category_json['#_+ve_reviews']))
	csv_list.append(str(category_json['#_-ve_reviews']))
	if category_json['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(category_json['helpfulness'] / category_json['total_reacted']))
	csv_list.append(str(category_json['rating']))
	csv_list.append(str(category_json['price']))
	csv_list.append(str(category_json['total_time']))
	csv_list.append(str(category_json['engaged_time']))
	csv_list.append(str(category_json['buy_again']))
	csv_list.append(str(category_json['#_products_related']['also_bought']))
	csv_list.append(str(category_json['#_products_related']['also_viewed']))
	csv_list.append(str(category_json['#_products_related']['bought_together']))

	csv_list.append(str(category_json['also_bought_influential']['#_reviews']))
	csv_list.append(str(category_json['also_bought_influential']['#_+ve_reviews']))
	csv_list.append(str(category_json['also_bought_influential']['#_-ve_reviews']))
	if category_json['also_bought_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(category_json['also_bought_influential']['helpfulness'] / category_json['also_bought_influential']['total_reacted']))
	csv_list.append(str(category_json['also_bought_influential']['rating']))
	csv_list.append(str(category_json['also_bought_influential']['price']))
	csv_list.append(str(category_json['also_bought_influential']['engaged_time']))
	csv_list.append(str(category_json['also_bought_influential']['buy_again']))
	csv_list.append(str(category_json['also_bought_influential']['#_products_related']['also_bought']))
	csv_list.append(str(category_json['also_bought_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(category_json['also_bought_influential']['#_products_related']['bought_together']))

	csv_list.append(str(category_json['also_viewed_influential']['#_reviews']))
	csv_list.append(str(category_json['also_viewed_influential']['#_+ve_reviews']))
	csv_list.append(str(category_json['also_viewed_influential']['#_-ve_reviews']))
	if category_json['also_viewed_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(category_json['also_viewed_influential']['helpfulness'] / category_json['also_viewed_influential']['total_reacted']))
	csv_list.append(str(category_json['also_viewed_influential']['rating']))
	csv_list.append(str(category_json['also_viewed_influential']['price']))
	csv_list.append(str(category_json['also_viewed_influential']['engaged_time']))
	csv_list.append(str(category_json['also_viewed_influential']['buy_again']))
	csv_list.append(str(category_json['also_viewed_influential']['#_products_related']['also_bought']))
	csv_list.append(str(category_json['also_viewed_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(category_json['also_viewed_influential']['#_products_related']['bought_together']))

	csv_list.append(str(category_json['bought_together_influential']['#_reviews']))
	csv_list.append(str(category_json['bought_together_influential']['#_+ve_reviews']))
	csv_list.append(str(category_json['bought_together_influential']['#_-ve_reviews']))
	if category_json['bought_together_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(category_json['bought_together_influential']['helpfulness'] / category_json['bought_together_influential']['total_reacted']))
	csv_list.append(str(category_json['bought_together_influential']['rating']))
	csv_list.append(str(category_json['bought_together_influential']['price']))
	csv_list.append(str(category_json['bought_together_influential']['engaged_time']))
	csv_list.append(str(category_json['bought_together_influential']['buy_again']))
	csv_list.append(str(category_json['bought_together_influential']['#_products_related']['also_bought']))
	csv_list.append(str(category_json['bought_together_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(category_json['bought_together_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_json['#_reviews']))
	csv_list.append(str(reviewer_json['#_products']))
	csv_list.append(str(reviewer_json['#_+ve_reviews']))
	csv_list.append(str(reviewer_json['#_-ve_reviews']))
	if reviewer_json['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_json['helpfulness'] / reviewer_json['total_reacted']))
	csv_list.append(str(reviewer_json['rating']))
	csv_list.append(str(reviewer_json['price']))
	csv_list.append(str(reviewer_json['total_time']))
	csv_list.append(str(reviewer_json['engaged_time']))
	csv_list.append(str(reviewer_json['buy_again']))
	
	######################################
	reviewer_json['fav_brand_name']
	reviewer_json['fav_subcategory_name']
	reviewer_json['fav_category_name']
	######################################

	csv_list.append(str(reviewer_json['fav_brand']))
	csv_list.append(str(reviewer_json['fav_subcategory']))
	csv_list.append(str(reviewer_json['fav_category']))
	csv_list.append(str(reviewer_json['#_related_bought']))
	csv_list.append(str(reviewer_json['#_related_brands_fav']))
	csv_list.append(str(reviewer_json['#_related_subcategory_fav']))
	csv_list.append(str(reviewer_json['#_related_category_fav']))
	csv_list.append(str(reviewer_json['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_json['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_json['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_json['also_bought_influential']['#_reviews']))
	csv_list.append(str(reviewer_json['also_bought_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_json['also_bought_influential']['#_-ve_reviews']))
	if reviewer_json['also_bought_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_json['also_bought_influential']['helpfulness'] / reviewer_json['also_bought_influential']['total_reacted']))
	csv_list.append(str(reviewer_json['also_bought_influential']['rating']))
	csv_list.append(str(reviewer_json['also_bought_influential']['price']))
	csv_list.append(str(reviewer_json['also_bought_influential']['engaged_time']))
	csv_list.append(str(reviewer_json['also_bought_influential']['buy_again']))
	csv_list.append(str(reviewer_json['also_bought_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_json['also_bought_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_json['also_bought_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_json['also_viewed_influential']['#_reviews']))
	csv_list.append(str(reviewer_json['also_viewed_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_json['also_viewed_influential']['#_-ve_reviews']))
	if reviewer_json['also_viewed_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_json['also_viewed_influential']['helpfulness'] / reviewer_json['also_viewed_influential']['total_reacted']))
	csv_list.append(str(reviewer_json['also_viewed_influential']['rating']))
	csv_list.append(str(reviewer_json['also_viewed_influential']['price']))
	csv_list.append(str(reviewer_json['also_viewed_influential']['engaged_time']))
	csv_list.append(str(reviewer_json['also_viewed_influential']['buy_again']))
	csv_list.append(str(reviewer_json['also_viewed_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_json['also_viewed_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_json['also_viewed_influential']['#_products_related']['bought_together']))
	
	csv_list.append(str(reviewer_json['bought_together_influential']['#_reviews']))
	csv_list.append(str(reviewer_json['bought_together_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_json['bought_together_influential']['#_-ve_reviews']))
	if reviewer_json['bought_together_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_json['bought_together_influential']['helpfulness'] / reviewer_json['bought_together_influential']['total_reacted']))
	csv_list.append(str(reviewer_json['bought_together_influential']['rating']))
	csv_list.append(str(reviewer_json['bought_together_influential']['price']))
	csv_list.append(str(reviewer_json['bought_together_influential']['engaged_time']))
	csv_list.append(str(reviewer_json['bought_together_influential']['buy_again']))
	csv_list.append(str(reviewer_json['bought_together_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_json['bought_together_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_json['bought_together_influential']['#_products_related']['bought_together']))

	reviewer_fav_brand_json = get_brand_json(reviewer_json['fav_brand_name'])
	reviewer_fav_subcategory_json = get_subcategory_json(tmp_category_dir, reviewer_json['fav_category_name'], reviewer_json['fav_subcategory_name'])
	reviewer_fav_category_json = get_category_json(tmp_category_dir, reviewer_json['fav_category_name'])

	csv_list.append(str(reviewer_fav_brand_json['#_reviews']))
	csv_list.append(str(reviewer_fav_brand_json['#_products']))
	csv_list.append(str(reviewer_fav_brand_json['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_brand_json['#_-ve_reviews']))
	if reviewer_fav_brand_json['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_brand_json['helpfulness'] / reviewer_fav_brand_json['total_reacted']))
	csv_list.append(str(reviewer_fav_brand_json['rating']))
	csv_list.append(str(reviewer_fav_brand_json['price']))
	csv_list.append(str(reviewer_fav_brand_json['total_time']))
	csv_list.append(str(reviewer_fav_brand_json['engaged_time']))
	csv_list.append(str(reviewer_fav_brand_json['buy_again']))
	csv_list.append(str(reviewer_fav_brand_json['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_brand_json['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_brand_json['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['#_reviews']))
	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['#_-ve_reviews']))
	if reviewer_fav_brand_json['also_bought_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['helpfulness'] / reviewer_fav_brand_json['also_bought_influential']['total_reacted']))
	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['rating']))
	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['price']))
	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['engaged_time']))
	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['buy_again']))
	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_brand_json['also_bought_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['#_reviews']))
	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['#_-ve_reviews']))
	if reviewer_fav_brand_json['also_viewed_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['helpfulness'] / reviewer_fav_brand_json['also_viewed_influential']['total_reacted']))
	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['rating']))
	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['price']))
	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['engaged_time']))
	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['buy_again']))
	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_brand_json['also_viewed_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['#_reviews']))
	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['#_-ve_reviews']))
	if reviewer_fav_brand_json['bought_together_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['helpfulness'] / reviewer_fav_brand_json['bought_together_influential']['total_reacted']))
	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['rating']))
	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['price']))
	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['engaged_time']))
	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['buy_again']))
	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_brand_json['bought_together_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_subcategory_json['#_reviews']))
	csv_list.append(str(reviewer_fav_subcategory_json['#_products']))
	csv_list.append(str(reviewer_fav_subcategory_json['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_subcategory_json['#_-ve_reviews']))
	if reviewer_fav_subcategory_json['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_subcategory_json['helpfulness'] / reviewer_fav_subcategory_json['total_reacted']))
	csv_list.append(str(reviewer_fav_subcategory_json['rating']))
	csv_list.append(str(reviewer_fav_subcategory_json['price']))
	csv_list.append(str(reviewer_fav_subcategory_json['total_time']))
	csv_list.append(str(reviewer_fav_subcategory_json['engaged_time']))
	csv_list.append(str(reviewer_fav_subcategory_json['buy_again']))
	csv_list.append(str(reviewer_fav_subcategory_json['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_subcategory_json['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_subcategory_json['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['#_reviews']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['#_-ve_reviews']))
	if reviewer_fav_subcategory_json['also_bought_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['helpfulness'] / reviewer_fav_subcategory_json['also_bought_influential']['total_reacted']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['rating']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['price']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['engaged_time']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['buy_again']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_bought_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['#_reviews']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['#_-ve_reviews']))
	if reviewer_fav_subcategory_json['also_viewed_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['helpfulness'] / reviewer_fav_subcategory_json['also_viewed_influential']['total_reacted']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['rating']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['price']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['engaged_time']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['buy_again']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_subcategory_json['also_viewed_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['#_reviews']))
	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['#_-ve_reviews']))
	if reviewer_fav_subcategory_json['bought_together_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['helpfulness'] / reviewer_fav_subcategory_json['bought_together_influential']['total_reacted']))
	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['rating']))
	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['price']))
	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['engaged_time']))
	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['buy_again']))
	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_subcategory_json['bought_together_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_category_json['#_reviews']))
	csv_list.append(str(reviewer_fav_category_json['#_products']))
	csv_list.append(str(reviewer_fav_category_json['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_category_json['#_-ve_reviews']))
	if reviewer_fav_category_json['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_category_json['helpfulness'] / reviewer_fav_category_json['total_reacted']))
	csv_list.append(str(reviewer_fav_category_json['rating']))
	csv_list.append(str(reviewer_fav_category_json['price']))
	csv_list.append(str(reviewer_fav_category_json['total_time']))
	csv_list.append(str(reviewer_fav_category_json['engaged_time']))
	csv_list.append(str(reviewer_fav_category_json['buy_again']))
	csv_list.append(str(reviewer_fav_category_json['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_category_json['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_category_json['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['#_reviews']))
	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['#_-ve_reviews']))
	if reviewer_fav_category_json['also_bought_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['helpfulness'] / reviewer_fav_category_json['also_bought_influential']['total_reacted']))
	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['rating']))
	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['price']))
	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['engaged_time']))
	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['buy_again']))
	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_category_json['also_bought_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['#_reviews']))
	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['#_-ve_reviews']))
	if reviewer_fav_category_json['also_viewed_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['helpfulness'] / reviewer_fav_category_json['also_viewed_influential']['total_reacted']))
	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['rating']))
	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['price']))
	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['engaged_time']))
	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['buy_again']))
	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_category_json['also_viewed_influential']['#_products_related']['bought_together']))

	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['#_reviews']))
	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['#_+ve_reviews']))
	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['#_-ve_reviews']))
	if reviewer_fav_category_json['bought_together_influential']['total_reacted'] == 0:
		csv_list.append(str(0))
	else:
		csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['helpfulness'] / reviewer_fav_category_json['bought_together_influential']['total_reacted']))
	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['rating']))
	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['price']))
	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['engaged_time']))
	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['buy_again']))
	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['#_products_related']['also_bought']))
	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['#_products_related']['also_viewed']))
	csv_list.append(str(reviewer_fav_category_json['bought_together_influential']['#_products_related']['bought_together']))

	io.write_line(csv_file_pointer, ','.join(csv_list) + '\n')
	csv_file_pointer.close()

def synch_data(paths, tmp_category_dir):
	csv_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/dataset.csv'
	written_no = 0

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
			#try:
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
			
			subcategory = '_'.join(categories[0][1:])
			if '/' in subcategory:
				subcategory = subcategory.replace('/', '')

			if '/' in category:
				category = category.replace('/', '')

			if '/' in brand:
				brand = brand.replace('/', '')

			if '/' in reviewerID:
				reviewerID = reviewerID.replace('/', '')

			if FLAG:
				sentiment, total_reacted, helpfulness, rating = get_review_details(helpful, reviewText, overall)
				date, month, year, day = unix_to_attributes(unixReviewTime)
				pos_senti, neg_senti = get_sentiment_scale(sentiment)
				price_scale = get_price_scale(price, category)

				if len(categories[0]) > 1:
					get_product_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories)
					get_brand_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories)
					get_subcategory_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories)
					get_category_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories)
					get_reviewer_details(tmp_category_dir, category, reviewerID, asin, pos_senti, neg_senti, total_reacted, helpfulness, rating, date, month, year, price_scale, related, brand, categories)
					product_json, brand_json, subcategory_json, category_json, reviewer_json = get_json(tmp_category_dir, asin, brand, subcategory, category, reviewerID)
					write_csv(tmp_category_dir, csv_file_path, product_json, brand_json, subcategory_json, category_json, reviewer_json, total_reacted, helpfulness, rating, date, month, year, day, price_scale, sentiment)
					save_global_data()
					written_no += 1
			line_no += 1
			same_category_flag = True
			#except:
			#	print('ERROR:\nFILE_PATH:', file_path, '\nLINE_NO:', line_no, '\nLINE:', merged_json)
			#	pass

		reading_file_pointer.close()
		print('Path no.', path_no, 'DONE:', file_path)
		path_no += 1
	print('Total numbers of reviews written is: ', written_no)

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
