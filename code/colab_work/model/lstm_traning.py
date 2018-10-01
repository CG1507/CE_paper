import io
import pandas as pd
import numpy as np
import math
import lstm
import keras.backend as K
from keras.callbacks import ModelCheckpoint, TensorBoard, ReduceLROnPlateau, EarlyStopping, LearningRateScheduler
from keras.models import load_model


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


""""
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

def lr_schedule(epoch):
	initial_lrate = 0.001
	k = 0.01
	lrate = initial_lrate * math.exp(-k*epoch)
	return lrate

def train_it(train_data_file_path, validation_data_file_path):
	batch_size = 1
	time_step = 100
	rows = 100
	attributes = 382

	weight_path = 'gdrive/My Drive/colab_training/code/model/partly_trained_1_300.h5'

	train_data_generator = KerasBatchGenerator(train_data_file_path, batch_size, time_step, rows, attributes)
	validation_data_generator = KerasBatchGenerator(validation_data_file_path, batch_size, time_step, rows, attributes)
	#model = lstm.lstm_model(time_step, attributes, weight_path)

	model = load_model(weight_path)
	print('partly trained partly_trained_1_300.h5 loaded.')

	#change period
	modelCheckpoint_call = ModelCheckpoint('model.{epoch:02d}-{val_loss:.2f}.hdf5', monitor='val_loss', verbose=1, save_best_only=False, save_weights_only=False, mode='auto', period=50)
	#earlyStopping_call = EarlyStopping(monitor='val_loss', min_delta=0, patience=5, verbose=1, mode='auto')
	lrDecay_call = LearningRateScheduler(lr_schedule, verbose=1)
	tensorboard_call = TensorBoard(log_dir='./logs', histogram_freq=0, batch_size=batch_size, write_graph=True, write_grads=True, write_images=True)
	#patience is changed
	#reduceLROnPlateau_call = ReduceLROnPlateau(monitor='val_loss', factor=0.1, patience=1, verbose=1, mode='auto')

	hist = model.fit_generator(train_data_generator.generate(),
						steps_per_epoch=train_data_generator.total_length//(batch_size * time_step),
						epochs=600,
						callbacks=[modelCheckpoint_call, lrDecay_call, tensorboard_call],
						validation_data=validation_data_generator.generate(),
						validation_steps=validation_data_generator.total_length//(batch_size * time_step),
						class_weight=None, max_queue_size=10, workers=1, use_multiprocessing=False, 
						verbose=1, shuffle=False, initial_epoch=300)

	model.save('partly_trained_301_600.h5')
	print('MODEL SAVED!')
	print(hist.history)

def test():
	#data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0.csv'
	#norm_data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0_norm.csv'
	#save_normalize_data(data_file_path)
	small_train_data_file_path = 'gdrive/My Drive/colab_training/final_data/train_dataset_2_norm.csv'
	small_validation_data_file_path = 'gdrive/My Drive/colab_training/final_data/validation_dataset_2_norm.csv'
	#small_train_data_file_path = 'gdrive/My\ Drive/colab_training/final_data/small_train_data.csv'
	#small_validation_data_file_path = 'gdrive/My\ Drive/colab_training/final_data/small_validation_data.csv'
	train_it(small_train_data_file_path, small_validation_data_file_path)

if __name__ == "__main__":
	test()