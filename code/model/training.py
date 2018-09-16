import model
from utils import io
from sklearn.preprocessing import MinMaxScaler
import pandas as pd

def normalize_data(data_file_path):
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

def test():
	data_file_path = '/media/dell/Seagate Expansion Drive/CE_paper/Implementation/final_data/dataset_0.csv'
	normalize_data(data_file_path)

if __name__ == "__main__":
	test()