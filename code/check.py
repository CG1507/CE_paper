import gzip
import json
import unixtime

def parse(path):
	g = gzip.open(path, 'r')
	for l in g:
		yield eval(l)

def test(file):
	items = []
	no_of_items = {}
	
	a= []
	line_no = 0
	l = 1
	for line in file:
		try:
			json_line = json.loads(json.dumps(line))
			unixReviewTime = str(json_line['unixReviewTime'])
			reviewerName = str(json_line['reviewerName'])
			reviewText = str(json_line['reviewText'])
			reviewTime = str(json_line['reviewTime'])
			summary = str(json_line['summary'])
			helpful = str(json_line['helpful'])
			overall = str(json_line['overall'])
			date = unixtime.convert(unixReviewTime)
			a.append(reviewText)
			print(l)
			l += 1
		except:
			pass

def main():
	file = parse("/media/dell/Seagate Expansion Drive/CE_paper/Amazon Dataset/categories/Books/reviews.json.gz")
	test(file)

if __name__ == "__main__":
	main()