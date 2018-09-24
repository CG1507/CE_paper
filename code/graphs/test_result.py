import graphs
from utils import io
import pandas as pd
import numpy as np
from keras.models import load_model

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
			
			if self.index + ((self.rows * self.time_step * self.batch_size) + self.rows) > self.total_length:
				self.index = 0

			for i in range(self.batch_size):
				for j in range(self.time_step):
					rows_temp = []
					for k in range(self.rows):
						cols_temp = []
						for l in range(self.cols):
							channel_temp = []
							for m in range(self.channel):
								channel_temp.append(self.data[self.index][l])
							cols_temp.append(channel_temp)
						rows_temp.append(cols_temp)
						self.index += 1

					x[i, j, :, :, :] = rows_temp
					y[i, j - 1, :, :, :] = rows_temp

				rows_temp = []
				for k in range(self.rows):
					cols_temp = []
					for l in range(self.cols):
						channel_temp = []
						for m in range(self.channel):
							channel_temp.append(self.data[self.index][l])
						cols_temp.append(channel_temp)
					rows_temp.append(cols_temp)
					self.index += 1
				y[i, -1, :, :, :] = rows_temp
				self.index -= self.rows
			yield x, y

def test(validation_data_file_path, model_file_path):
	batch_size = 1
	time_step = 100
	rows = 10
	cols = 382
	channel = 1
	
	model = load_model(model_file_path)
	

def run():
	model_file_path = ''
	validation_data_file_path = 'gdrive/My Drive/colab_training/final_data/validation_dataset_0_norm.csv'
	test(validation_data_file_path, model_file_path)

if __name__ == "__main__":
	run()