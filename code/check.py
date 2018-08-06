import gzip
import json

def parse(path):
	"""

	Args:

	Returns:

	"""
	g = gzip.open(path, 'r')
	for l in g:
		yield eval(l)

def no_items_reviewed(file):
	"""

	Args:

	Returns:
	
	"""
	items = []
	no_of_items = {}
	
	line_no = 0
	for line in file:
		json_line = json.loads(json.dumps(line))
		item = str(json_line['asin'])
	
		if item in items:
			no_of_items[item] += 1
		else:
			items.append(item)
			no_of_items[item] = 1
		
		line_no += 1
		if line_no % 100000 == 0:
			print(line_no)
			print(no_of_items)
			print(len(items))

	return items, no_of_items

def main():
	file = parse("/media/dell/Seagate Expansion Drive/CE_paper/Amazon Dataset/categories/Books/reviews.json.gz")
	items, no_of_items = no_items_reviewed(file)
	print(no_of_items)
	print(items)
	print(len(items))
	print(len(no_of_items))

if __name__ == "__main__":
	main()