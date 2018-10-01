#import graphs
#from utils import io
import io
from keras.models import load_model
from keras.models import Model
import pandas as pd
import numpy as np
import pickle
import keras.backend as K
import matplotlib.pyplot as plt


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

"""
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
			
			if self.index + (self.batch_size * self.time_step * self.rows) >= self.total_length:
				self.index = 0

			for i in range(self.batch_size):
				for j in range(self.time_step):
					x[i, j, :] = np.sum(self.data[self.index:self.index + self.rows], axis=0)
					y[i, j - 1, :] = np.sum(self.data[self.index:self.index + self.rows], axis=0)
					self.index += self.rows

				y[i, -1, :] = np.sum(self.data[self.index:self.index + self.rows], axis=0)

			yield x, y
"""

def save_graph_data(model, validation_data_generator, val_steps):
	no = 1
	for x, y in validation_data_generator.generate():
		if no > val_steps:
			break
		pred_val = model.predict_on_batch(x)
		print(pred_val)
		
		with open('predicted' + str(no) + '.pkl', 'wb') as writing_file_pointer:
			pickle.dump([x, pred_val], writing_file_pointer)
		no += 1

def test(validation_data_file_path, model_file_path):
	batch_size = 1
	time_step = 100
	rows = 100
	attributes = 382

	validation_data_generator = KerasBatchGenerator(validation_data_file_path, batch_size, time_step, rows, attributes)
	val_steps = validation_data_generator.total_length//(batch_size * time_step)
	model = load_model(model_file_path)
	print('model loaded')
	
	#import conv_lstm
	#weight_path = '/home/dell/Desktop/weights.180.hdf5'
	#weight_path = 'gdrive/My Drive/colab_training/code/model/weights.180.hdf5'
	#model = conv_lstm.conv_lstm_2d(weight_path)

	#model = Model(inputs=model.get_layer('conv_lst_m2d_1').input, outputs=model.get_layer('batch_normalization_4').output)
	#save_graph_data(model, validation_data_generator, val_steps)

	temp = []
	temp_sum = []
	no = 1
	for x, y in validation_data_generator.generate():
		if no >= val_steps:
			break
		pred = model.predict(x)

		sum_x = 0
		sum_pred = 0
		for i in range(time_step):
			sum_x += y[0, i, 0]
			sum_pred += pred[0, i, 0]

			#temp.append(x[0, i, 250])
			#temp_sum.append(pred[0, i, 250])

		temp.append(sum_x)
		temp_sum.append(sum_pred)

		no += 1

	print(temp, temp_sum)

	plt.plot([(i + 1) for i in range(len(temp))], temp)
	#plt.show()
	plt.plot([(i + 1) for i in range(len(temp_sum))], temp_sum, 'g')
	plt.show()

def run():
	#model_file_path = '/home/dell/Desktop/model.100-0.02.hdf5'
	model_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/training/lstm_sum_scale_data2/model.300-0.01.hdf5'
	validation_data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/validation_dataset_2_norm.csv'
	#validation_data_file_path = 'gdrive/My Drive/colab_training/final_data/validation_dataset_0_norm.csv'
	test(validation_data_file_path, model_file_path)

if __name__ == "__main__":
	run()