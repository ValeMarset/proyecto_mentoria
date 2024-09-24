from config.db_config import db_connection


def load_to_database(db_connection):
	db_connection.connect()
	# # Ejemplo de inserción de datos:
	# # cursor = db_connection.cursor
	# # for index, row in df.iterrows():
	# #     cursor.execute("INSERT INTO ... VALUES (...)", ...)
	# # db_connection.connection.commit()
	# db_connection.close()

