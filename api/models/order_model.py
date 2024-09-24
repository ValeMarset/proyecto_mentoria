


class Order:
	def __init__(self, id, product_id, customer_id, order_date, fecha_entrega_id, fecha_envio_id, order_status,
	             order_total, clima_id, conversion_moneda_id):
		self.id = id
		self.product_id = product_id
		self.customer_id = customer_id
		self.order_date = order_date
		self.fecha_entrega_id = fecha_entrega_id
		self.fecha_envio_id = fecha_envio_id
		self.order_status = order_status
		self.order_total = order_total
		self.clima_id = clima_id
		self.conversion_moneda_id = conversion_moneda_id