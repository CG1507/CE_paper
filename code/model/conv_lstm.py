import model
from keras.models import Sequential
from keras.layers.convolutional import Conv3D
from keras.layers.convolutional_recurrent import ConvLSTM2D
from keras.layers.normalization import BatchNormalization
from keras.optimizers import Adam
import numpy as np

def conv_lstm_2d(weight_path=None):
	seq = Sequential()
	seq.add(ConvLSTM2D(filters=10, kernel_size=(7, 7), input_shape=(None, 10, 382, 1), padding='same', return_sequences=True))
	seq.add(BatchNormalization())

	#seq.add(ConvLSTM2D(filters=20, kernel_size=(7, 7), padding='same', return_sequences=True))
	#seq.add(BatchNormalization())

	#seq.add(ConvLSTM2D(filters=30, kernel_size=(7, 7), padding='same', return_sequences=True))
	#seq.add(BatchNormalization())

	#seq.add(ConvLSTM2D(filters=50, kernel_size=(7, 7), padding='same', return_sequences=True))
	#seq.add(BatchNormalization())

	seq.add(Conv3D(filters=1, kernel_size=(7, 7, 7), activation='sigmoid', padding='same', data_format='channels_last'))
	
	optimizer = Adam(lr=0.1, beta_1=0.9, beta_2=0.999, epsilon=None, decay=0.0, amsgrad=False)
	seq.compile(loss='mae', optimizer=optimizer, metrics=['mae', 'acc'])
	seq.summary()
	
	if weight_path != None:
		seq.load_weights(weight_path)
	
	return seq

def test():
	conv_lstm_2d()

if __name__ == "__main__":
	test()