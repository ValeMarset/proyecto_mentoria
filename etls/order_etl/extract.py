import os
import pandas as pd


def extract_all_jsons(data_dir):
	"""
    Lee todos los archivos JSON en un directorio y combina sus datos en un solo DataFrame.

    Parameters:
    data_dir (str): El directorio donde se encuentran los archivos JSON.

    Returns:
    pd.DataFrame: Un DataFrame que contiene los datos combinados de todos los archivos JSON.
    """
	if not os.path.isdir(data_dir):
		raise ValueError(f"El directorio especificado no existe: {data_dir}")

	all_data_frames = []

	for filename in os.listdir(data_dir):
		if filename.endswith('.json'):
			file_path = os.path.join(data_dir, filename)
			try:
				df = pd.read_json(file_path, lines=True, orient='records')
				print(f"Archivo procesado: {filename}")
				all_data_frames.append(df)
			except ValueError as e:
				print(f"Error al leer el archivo {filename}: {e}")
			except Exception as e:
				print(f"Error inesperado al procesar el archivo {filename}: {e}")

	if all_data_frames:
		combined_df = pd.concat(all_data_frames, ignore_index=True)
	else:
		combined_df = pd.DataFrame()

	return combined_df
