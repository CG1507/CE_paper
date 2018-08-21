import gzip
import json
import unixtime

def parse(path):
	g = gzip.open(path, 'r')
	for l in g:
		yield eval(l)

def link(file_pointer):

	for line in file_pointer:
		json_line = json.loads(json.dumps(line))
		unixReviewTime = str(json_line['unixReviewTime'])

def test():
	file_pointer = parse("/media/dell/Seagate Expansion Drive/CE_paper/Amazon Dataset/categories/Books/reviews.json.gz")
	link(file_pointer)

if __name__ == "__main__":
	test()