from keras.models import Sequential
from keras.layers import Dense, TimeDistributed, Input
from keras.layers import LSTM
from keras.layers import Activation, Dense
from keras.models import Model
from keras.optimizers import Adam
from keras import regularizers
from keras import backend as K

def lstm_model(time_step, attributes, weight_path=None):
	input_word = Input(batch_shape=(None, time_step, attributes), dtype='float32', name='input')
	hidden_units1 = LSTM(units=512, activation='tanh', recurrent_activation='hard_sigmoid', use_bias=True,  
					kernel_initializer='glorot_uniform', recurrent_initializer='orthogonal', bias_initializer='zeros', 
					unit_forget_bias=True, dropout=0.2, recurrent_dropout=0.2, implementation=1, return_sequences=True,
					name='lstm1')(input_word)

	"""
	hidden_units2 = LSTM(units=1024, activation='tanh', recurrent_activation='hard_sigmoid', use_bias=True,  
					kernel_initializer='glorot_uniform', recurrent_initializer='orthogonal', bias_initializer='zeros', 
					unit_forget_bias=True, dropout=0.5, recurrent_dropout=0.5, implementation=1, return_sequences=True, 
					kernel_regularizer=regularizers.l2(0.01), recurrent_regularizer=regularizers.l2(0.01),
					bias_regularizer=regularizers.l2(0.01), activity_regularizer=regularizers.l1(0.01),
					name='lstm2')(hidden_units1)

	hidden_units3 = LSTM(units=512, activation='tanh', recurrent_activation='hard_sigmoid', use_bias=True,  
					kernel_initializer='glorot_uniform', recurrent_initializer='orthogonal', bias_initializer='zeros', 
					unit_forget_bias=True, dropout=0.5, recurrent_dropout=0.5, implementation=1, return_sequences=True, 
					kernel_regularizer=regularizers.l2(0.01), recurrent_regularizer=regularizers.l2(0.01),
					bias_regularizer=regularizers.l2(0.01), activity_regularizer=regularizers.l1(0.01),
					name='lstm3')(hidden_units2)

	hidden_units4 = LSTM(units=382, activation='tanh', recurrent_activation='hard_sigmoid', use_bias=True,  
					kernel_initializer='glorot_uniform', recurrent_initializer='orthogonal', bias_initializer='zeros', 
					unit_forget_bias=True, dropout=0.5, recurrent_dropout=0.5, implementation=1, return_sequences=True, 
					kernel_regularizer=regularizers.l2(0.01), recurrent_regularizer=regularizers.l2(0.01),
					bias_regularizer=regularizers.l2(0.01), activity_regularizer=regularizers.l1(0.01),
					name='lstm4')(hidden_units3)
	"""

	denselayer = Dense(attributes)(hidden_units1)
	last_layer = Activation('sigmoid')(denselayer)

	model = Model(input=input_word, output=last_layer)

	optimizer = Adam(lr=0.001, beta_1=0.9, beta_2=0.999, epsilon=None, decay=0.0, amsgrad=False)
	model.compile(loss='mse', optimizer='adam', metrics=['mse', 'acc'])

	if weight_path != None:
		model.load_weights(weight_path, by_name=True)
		print('model weights is restored.')

	model.summary()

	return model

def test():
	time_step = 100
	attributes = 382
	weight_path=None
	model = lstm_model(time_step, attributes, weight_path)

if __name__ == "__main__":
	test()