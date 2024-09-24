import psycopg2
import psycopg2.extras


class ConnectionDb:
	def __init__(self, host, port, user, password, database):
		"""Inicializa la conexión a la base de datos y crea un cursor."""
		self.host = host
		self.port = port
		self.user = user
		self.password = password
		self.database = database
		self.connection = None
		self.cursor = None

	def connect(self):
		"""Abre la conexión a la base de datos y crea un cursor."""
		try:
			self.connection = psycopg2.connect(
				host=self.host,
				port=self.port,
				user=self.user,
				password=self.password,
				database=self.database
			)
			self.cursor = self.connection.cursor()
			print('Conexión a Postgres establecida exitosamente')
		except psycopg2.DatabaseError as e:
			print(f'Error al conectar a la base de datos: {e}')

	def close(self):
		"""Cierra la conexión y el cursor."""
		if self.cursor is not None:
			self.cursor.close()
		if self.connection is not None:
			self.connection.close()
		print('Conexión a Postgres cerrada')

	def test_connection(self):
		"""Verifica si la conexión a la base de datos está activa utilizando el estado de la conexión."""
		if self.connection is not None:
			if self.connection.closed == 0:
				print('La conexión está activa')
			else:
				print('La conexión está cerrada')
		else:
			print('La conexión no fue establecida')

	def insert_records(self, table, columns, records):
		"""Inserta múltiples registros en una tabla dada."""
		if self.connection is not None and self.cursor is not None:
			try:
				column_names = ', '.join(columns)
				placeholders = ', '.join(['%s'] * len(columns))
				insert_query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"

				self.cursor.executemany(insert_query, records)
				self.connection.commit()
				print(f'Inserción exitosa en la tabla {table}')
			except psycopg2.DatabaseError as e:
				print(f'Error al insertar registros en la tabla {table}: {e}')
		else:
			print('La conexión no fue establecida')
