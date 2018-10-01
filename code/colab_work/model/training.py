import io
#from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
import math
import conv_lstm
import keras.backend as K
from keras.callbacks import ModelCheckpoint, TensorBoard, ReduceLROnPlateau, EarlyStopping, LearningRateScheduler
from keras.models import load_model

"""
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
"""

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

def lr_schedule(epoch):
	initial_lrate = 0.1
	k = 0.09
	lrate = initial_lrate * math.exp(-k*epoch)
	return lrate

def train_it(train_data_file_path, validation_data_file_path):
	batch_size = 1
	time_step = 100
	rows = 10
	cols = 382
	channel = 1
	weight_path = None#'gdrive/My Drive/colab_training/code/model/weights.180.hdf5'

	train_data_generator = KerasBatchGenerator(train_data_file_path, batch_size, time_step, rows, cols, channel)
	validation_data_generator = KerasBatchGenerator(validation_data_file_path, batch_size, time_step, rows, cols, channel)
	model = conv_lstm.conv_lstm_2d(weight_path)
	#model = load_model(weight_path)
	#print('partly trained model.180-0.72.hdf5 loaded.')

	#change period
	modelCheckpoint_call = ModelCheckpoint('model.{epoch:02d}-{val_loss:.2f}.hdf5', monitor='val_loss', verbose=1, save_best_only=False, save_weights_only=False, mode='auto', period=1)
	#earlyStopping_call = EarlyStopping(monitor='val_loss', min_delta=0, patience=5, verbose=1, mode='auto')
	lrDecay_call = LearningRateScheduler(lr_schedule, verbose=1)
	tensorboard_call = TensorBoard(log_dir='./logs', histogram_freq=0, batch_size=batch_size, write_graph=True, write_grads=True, write_images=True)
	#patience is changed
	#reduceLROnPlateau_call = ReduceLROnPlateau(monitor='val_loss', factor=0.1, patience=1, verbose=1, mode='auto')

	hist = model.fit_generator(train_data_generator.generate(),
						steps_per_epoch=train_data_generator.total_length//((time_step * rows * batch_size) + rows),
						epochs=30,
						callbacks=[modelCheckpoint_call, lrDecay_call, tensorboard_call],
						validation_data=validation_data_generator.generate(),
						validation_steps=validation_data_generator.total_length//((time_step * rows * batch_size) + rows),
						class_weight=None, max_queue_size=10, workers=1, use_multiprocessing=False, 
						verbose=1, shuffle=False, initial_epoch=0)

	model.save('partly_trained_1_30.h5')
	print('MODEL SAVED!')
	print(hist.history)

def test():
	#data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0.csv'
	#norm_data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0_norm.csv'
	#save_normalize_data(data_file_path)
	small_train_data_file_path = 'gdrive/My Drive/colab_training/final_data/train_dataset_0_norm.csv'
	small_validation_data_file_path = 'gdrive/My Drive/colab_training/final_data/validation_dataset_0_norm.csv'
	#small_train_data_file_path = 'gdrive/My\ Drive/colab_training/final_data/small_train_data.csv'
	#small_validation_data_file_path = 'gdrive/My\ Drive/colab_training/final_data/small_validation_data.csv'
	train_it(small_train_data_file_path, small_validation_data_file_path)

if __name__ == "__main__":
	test()