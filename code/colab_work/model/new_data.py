import codecs
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
import math

def save_normalize_data(data_file_path):
	dataset = pd.read_csv(data_file_path)
	values = dataset.values
	print('Reading done')
	no = len(values)//100
	print('Total rows:', no)

	temp = []
	index = 0
	for i in range(no):
		temp.append(np.sum(values[index:index + 100], axis=0))
		index += 100

	temp = np.array(temp)

	scaler = MinMaxScaler(feature_range=(0, 1))
	scaled = scaler.fit_transform(temp)
	print('transform done')

	writing_data_file_pointer  = codecs.open('dataset_2_norm.csv', 'w', 'utf-8')
	for row in scaled:
		new_row = []
		for val in row:
			new_row.append(str(val))
		writing_data_file_pointer.write(str(','.join(new_row)) + '\n')

	dataset = pd.read_csv('dataset_2_norm.csv')
	values = dataset.values
	print(values.shape)

class KerasBatchGenerator(object):
	def __init__(self, file_path, batch_size, time_step, rows, attributes):
		self.file_pointer = pd.read_csv(file_path)
		self.data = self.file_pointer.values
		self.batch_size = batch_size
		self.time_step = time_step
		self.rows = rows
		self.attributes = attributes
		self.index = 0
		self.total_length = len(self.data)

	def generate(self):
		while True:
			x = np.zeros((self.batch_size, self.time_step, self.attributes))
			y = np.zeros((self.batch_size, self.time_step, self.attributes))
			
			if self.index + (self.batch_size * self.time_step) >= self.total_length:
				self.index = 0

			for i in range(self.batch_size):
				for j in range(self.time_step):
					x[i, j, :] = self.data[self.index]
					y[i, j - 1, :] = self.data[self.index]
					self.index += 1

				y[i, -1, :] = self.data[self.index]

			yield x, y

def test():
	data_file_path = 'gdrive/My Drive/colab_training/final_data/dataset_0.csv'
	#norm_data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0_norm.csv'
	save_normalize_data(data_file_path)
	#small_train_data_file_path = 'gdrive/My Drive/colab_training/final_data/train_dataset_0_norm.csv'
	#small_validation_data_file_path = 'gdrive/My Drive/colab_training/final_data/validation_dataset_0_norm.csv'
	#small_train_data_file_path = 'gdrive/My\ Drive/colab_training/final_data/small_train_data.csv'
	#small_validation_data_file_path = 'gdrive/My\ Drive/colab_training/final_data/small_validation_data.csv'
	#train_it(small_train_data_file_path, small_validation_data_file_path)

if __name__ == "__main__":
	test()