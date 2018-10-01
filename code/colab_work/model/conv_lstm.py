import model
from keras.models import Sequential
from keras.layers.convolutional import Conv3D
from keras.layers.convolutional_recurrent import ConvLSTM2D
from keras.layers.normalization import BatchNormalization
from keras.layers import Activation
from keras.optimizers import Adam
from keras import regularizers
import numpy as np

def conv_lstm_2d(weight_path=None):
	seq = Sequential()
	seq.add(ConvLSTM2D(filters=8, kernel_size=(7, 7), input_shape=(None, 10, 382, 1), padding='same', return_sequences=True, kernel_regularizer=regularizers.l2(0.01), recurrent_regularizer=regularizers.l2(0.01), bias_regularizer=regularizers.l2(0.01), activity_regularizer=regularizers.l1(0.01), dropout=0.5, recurrent_dropout=0.5))
	seq.add(BatchNormalization())

	seq.add(ConvLSTM2D(filters=16, kernel_size=(7, 7), padding='same', return_sequences=True, kernel_regularizer=regularizers.l2(0.01), recurrent_regularizer=regularizers.l2(0.01), bias_regularizer=regularizers.l2(0.01), activity_regularizer=regularizers.l1(0.01), dropout=0.5, recurrent_dropout=0.5))
	seq.add(BatchNormalization())

	seq.add(ConvLSTM2D(filters=32, kernel_size=(7, 7), padding='same', return_sequences=True, kernel_regularizer=regularizers.l2(0.01), recurrent_regularizer=regularizers.l2(0.01), bias_regularizer=regularizers.l2(0.01), activity_regularizer=regularizers.l1(0.01), dropout=0.5, recurrent_dropout=0.5))
	seq.add(BatchNormalization())

	seq.add(ConvLSTM2D(filters=1, kernel_size=(7, 7), padding='same', return_sequences=True, kernel_regularizer=regularizers.l2(0.01), recurrent_regularizer=regularizers.l2(0.01), bias_regularizer=regularizers.l2(0.01), activity_regularizer=regularizers.l1(0.01), dropout=0.5, recurrent_dropout=0.5))
	#seq.add(BatchNormalization())

	#seq.add(Conv3D(filters=1, kernel_size=(3, 3, 3), activation='sigmoid', padding='same', data_format='channels_last'))#, kernel_regularizer=regularizers.l2(0.01), bias_regularizer=regularizers.l2(0.01), activity_regularizer=regularizers.l1(0.01)))
	#seq.add(Activation('sigmoid'))

	optimizer = Adam(lr=0.001, beta_1=0.9, beta_2=0.999, epsilon=None, decay=0.0, amsgrad=False)
	seq.compile(loss='mse', optimizer=optimizer, metrics=['mse', 'acc'])
	seq.summary()

	if weight_path != None:
		seq.load_weights(weight_path, by_name=True)
		print('model weights is restored.')

	return seq

def test():
	weight_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/code/model/weights/weights.28-0.01.hdf5'
	seq = conv_lstm_2d(weight_path)
	seq.save('partly_trained.h5')

if __name__ == "__main__":
	test()