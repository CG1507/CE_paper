import datasets
from utils import io
import gzip
import json
import pickle
import synch

def test():
	tmp_category_dir = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/tmp_data/categories/'
	category_names = ['Electronics']
	paths = get_sorted_files_path(tmp_category_dir, category_names)

if __name__ == "__main__":
	test()