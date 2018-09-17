import model
from utils import io
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
import math
import conv_lstm
from keras.callbacks import ModelCheckpoint, TensorBoard, ReduceLROnPlateau, EarlyStopping

def save_normalize_data(data_file_path):
	dataset = pd.read_csv(data_file_path)
	values = dataset.values	
	print('Reading done')

	scaler = MinMaxScaler(feature_range=(0, 1))
	scaled = scaler.partial_fit(values)
	print('transform done')

	writing_data_file_pointer  = io.create_file('/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0_norm.csv')
	for row in scaled:
		new_row = []
		for val in row:
			new_row.append(str(val))
		io.write_line(writing_data_file_pointer, str(','.join(new_row)) + '\n')

class KerasBatchGenerator(object):
	def __init__(self, file_path, batch_size, time_step, rows, cols, channel):
		self.file_pointer = pd.read_csv(file_path)
		self.data = self.file_pointer.values
		self.batch_size = batch_size
		self.time_step = time_step
		self.rows = rows
		self.cols = cols
		self.channel = channel
		self.index = 0
		self.total_length = len(self.data)

	def generate(self):
		while True:
			x = np.zeros((self.batch_size, self.time_step, self.rows, self.cols, self.channel))
			y = np.zeros((self.batch_size, self.time_step, self.rows, self.cols, self.channel))
			
			if self.index + (self.rows * (self.time_step + 1) * self.batch_size) >= self.total_length:
				self.index = 0

			for i in range(self.batch_size):
				for j in range(self.time_step):
					rows_temp = []
					for k in range(self.rows):
						cols_temp = []
						for l in range(self.cols):
							channel_temp = []
							for m in range(self.channel):
								channel_temp.append(self.data[k + self.index][l])
							cols_temp.append(channel_temp)
						rows_temp.append(cols_temp)
					self.index += 1

					x[i, j, :, :, :] = rows_temp
					y[i, j - 1, :, :, :] = rows_temp

			for i in range(self.batch_size):
				rows_temp = []
				for k in range(self.rows):
					cols_temp = []
					for l in range(self.cols):
						channel_temp = []
						for m in range(self.channel):
							channel_temp.append(self.data[k + self.index][l])
						cols_temp.append(channel_temp)
					rows_temp.append(cols_temp)
				self.index += 1
				y[i, -1, :, :, :] = rows_temp
			self.index -= (self.rows * self.batch_size)
				
			yield x, y

def loss_callback():
	initial_lrate = 0.1
	drop = 0.5
	epochs_drop = 10.0
	lrate = initial_lrate * math.pow(drop, math.floor((1+epoch)/epochs_drop))
	return lrate

def test():
	data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0.csv'
	norm_data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0_norm.csv'
	#save_normalize_data(data_file_path)
	small_data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/small_data.csv'
	train_it(small_data_file_path)

if __name__ == "__main__":
	test()