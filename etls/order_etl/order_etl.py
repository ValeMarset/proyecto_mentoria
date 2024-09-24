import os
import pandas as pd
from extract import extract_all_jsons
from transform import handler_transform
# from load import load_data


def main():
	current_dir = os.path.dirname(__file__)
	data_dir = os.path.join(current_dir, '..', '..', 'data', 'raw')

	# Extracción
	df = extract_all_jsons(data_dir)

	# Transformación
	result = handler_transform(df)

	# Carga


if __name__ == "__main__":
	main()
