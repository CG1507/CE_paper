import io
import pandas as pd
import numpy as np
import math
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
			
			if self.index + (self.batch_size * self.time_step * self.rows) >= self.total_length:
				self.index = 0

			for i in range(self.batch_size):
				for j in range(self.time_step):
					x[i, j, :] = np.sum(self.data[self.index:self.index + self.rows], axis=0)
					y[i, j - 1, :] = np.sum(self.data[self.index:self.index + self.rows], axis=0)
					self.index += self.rows

				y[i, -1, :] = np.sum(self.data[self.index:self.index + self.rows], axis=0)

			yield x, y

def test():
	batch_size = 1
	time_step = 100
	rows = 100
	attributes = 382
	validation_data_filepath = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/validation_dataset_0_norm.csv'
	validation_data_generator = KerasBatchGenerator(validation_data_filepath, batch_size, time_step, rows, attributes)
	validation_steps = validation_data_generator.total_length // (batch_size * time_step * rows)
	temp = []
	
	no = 1
	for x, y in validation_data_generator.generate():
		if no >= validation_steps:
			break
		for i in range(time_step):
			temp.append(x[0, i, 0])
		no += 1

	print(temp)

	plt.plot([(i + 1) for i in range(len(temp))], temp)
	plt.show()

if __name__ == "__main__":
	test()