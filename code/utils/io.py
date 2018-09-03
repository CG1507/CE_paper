import codecs
import os

def make_dir(dir_list, dir_location):
	"""
	It creates list of empty folders for given location.

	Args:
		dir_list (list): Folder names that are going to be create.
		dir_location (string): Address where all folder is going to be created.

	"""
	for item in dir_list:
		directory = dir_location + item 
		if not os.path.exists(directory):
			os.makedirs(directory)

def create_file(file_address):
	"""
	This function creates file and return filepointer.

	Args:
		file_address (string): File path with file name.

	Returns:
		fout (codecs object): Codecs file object.

	"""
	mode = 'w'
	fout = codecs.open(file_address, mode, 'utf-8')
	return fout

def read_file(file_address):
	"""
	This function reads file and return filepointer.

	Args:
		file_address (string): File path with file name.

	Returns:
		fout (codecs object): Codecs file object.

	"""
	mode = 'r'
	fout = codecs.open(file_address, mode, 'utf-8')
	return fout

def append_file(file_address):
	"""
	This function append file and return filepointer.

	Args:
		file_address (string): File path with file name.

	Returns:
		fout (codecs object): Codecs file object.
	"""
	mode = 'a'
	fout = codecs.open(file_address, mode, 'utf-8')
	return fout

def write_line(file_pointer, line):
	"""
	This function writes given string in file.

	Args:
		file_pointer (codecs object): codec file object.
		line (string): line wants to write.

	"""
	file_pointer.write(str(line))

def list_files(folder_address, ends_with = ".txt"):
	"""
	This function reads file having given suffix from folder_address.

	Args:
		folder_address (string): folder address from where all files lies.
		ends_with (string): suffix of files.
	"""
	return [file.split('.')[-2] for file in os.listdir(folder_address) if file.endswith(ends_with)]

def list_dirs(folder_address):
	
	return [file for file in os.listdir(folder_address)]

def file_presence(filepath):
	return os.path.exists(filepath)