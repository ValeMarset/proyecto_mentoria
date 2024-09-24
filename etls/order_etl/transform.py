import pandas as pd
import sys


def normalize_orders(df):
    """Normaliza el DataFrame de órdenes y detalles."""
    orders_df = pd.json_normalize(
        df['order'],
        record_path='details',
        meta=['order_id', 'order_date', 'order_status', 'order_total'],
        errors='ignore',
        sep='_'
    )

    orders_df.rename(columns={
        'fecha_entrega': 'delivery_date_id',
        'fecha_envio': 'shipping_date_id',
        'order_id': 'id'
    }, inplace=True)

    return orders_df[
        ['id', 'customer_id', 'delivery_date_id', 'shipping_date_id', 'order_date', 'order_status', 'order_total']]


def extract_product_data(df):
    """Extrae y normaliza los datos de productos."""
    product_df = pd.json_normalize(df['order'], 'details', ['order_id'], sep='_')
    product_df = product_df[['brand_product', 'name_prodcuct', 'precio', 'status']].drop_duplicates()
    product_df.rename(columns={'precio': 'price', 'name_prodcuct': 'name_product'}, inplace=True)
    product_df['id'] = pd.RangeIndex(start=1, stop=len(product_df) + 1)
    return product_df


def extract_customer_address_data(df):
    """Extrae y normaliza los datos de direcciones de clientes."""
    customer_address_df = pd.json_normalize(df['order'], 'details', ['order_id'], sep='_')
    customer_address_df = customer_address_df[
        ['customer_address', 'customer_region', 'customer_nation']].drop_duplicates()

    customer_address_df = customer_address_df.rename(columns={
        'customer_address': 'address',
        'customer_region': 'region'
    })

    customer_address_df['id'] = pd.RangeIndex(start=1, stop=len(customer_address_df) + 1)

    unique_nations = customer_address_df[['customer_nation']].drop_duplicates().reset_index(drop=True)
    unique_nations.columns = ['name']
    unique_nations['id'] = pd.RangeIndex(start=1, stop=len(unique_nations) + 1)

    return customer_address_df, unique_nations[['name', 'id']]


def map_customer_address_to_country(customer_address_df, country_mapping):
    """Une las direcciones de clientes con el mapeo de países."""
    customer_address_df = pd.merge(customer_address_df, country_mapping, left_on='customer_nation', right_on='customer_nation', how='left')

    return customer_address_df[['id', 'address', 'region', 'country_id']].drop_duplicates()


def extract_customer_data(df, customer_address_df):
    """Extrae y normaliza los datos de clientes."""

    customer_df = pd.json_normalize(df['order'], 'details', ['order_id'], sep='_')
    customer_df = customer_df[['customer_id', 'customer_name', 'customer_phone', 'customer_address']]

    customer_df = pd.merge(customer_df, customer_address_df[['address', 'id']],
                           left_on='customer_address', right_on='address', how='left')

    customer_df.rename(columns={
        'customer_id': 'id',
        'customer_name': 'name',
        'customer_phone': 'phone',
        'id': 'address_id'
    }, inplace=True)

    return customer_df[['id', 'name', 'phone', 'address_id']].drop_duplicates()


def extract_all_dates(df):
    """
    Extrae todas las fechas (order_date, fecha_entrega, fecha_envio) para insertar en la tabla time.
    """

    details_df = pd.json_normalize(df['order'], 'details', ['order_id'], sep='_')

    order_dates = pd.DataFrame({
        'date': pd.to_datetime(df['order'].apply(lambda x: x['order_date'])),
        'type': 'order_date'
    })

    delivery_dates = pd.DataFrame({
        'date': pd.to_datetime(details_df['fecha_entrega'].rename('delivery_date', inplace=True)),
        'type': 'delivery_date'
    })

    shipping_dates = pd.DataFrame({
        'date': pd.to_datetime(details_df['fecha_envio'].rename('shipping_date', inplace=True)),
        'type': 'shipping_date'
    })

    all_dates_df = pd.concat([order_dates, delivery_dates, shipping_dates]).drop_duplicates().reset_index(drop=True)

    all_dates_df['month'] = all_dates_df['date'].dt.month
    all_dates_df['year'] = all_dates_df['date'].dt.year

    all_dates_df['id'] = pd.RangeIndex(start=1, stop=len(all_dates_df) + 1)

    return all_dates_df


def extract_supplier_data(df):
    """Extrae y normaliza los datos de proveedores."""

    supplier_df = pd.json_normalize(df['order'], 'details', ['order_id'], sep='_')
    supplier_df = supplier_df[['supplier_name']].drop_duplicates()
    supplier_df['id'] = pd.RangeIndex(start=1, stop=len(supplier_df) + 1)

    return supplier_df


def extract_order_details_data(df, supplier_df, product_df):
    """Extrae y normaliza los detalles de las órdenes."""

    order_details_df = pd.json_normalize(df['order'], 'details', ['order_id'], sep='_')

    order_details_df.rename(columns={
        'cantidad': 'quantity',
        'descuento': 'discount',
        'flag_devolución': 'return_flag',
        'impuesto': 'tax',
        'numero_item': 'item_number',
        'name_prodcuct': 'name_product'
    }, inplace=True)

    order_details_df = order_details_df[[
        'order_id', 'quantity', 'discount', 'return_flag', 'tax',
        'item_number', 'supplier_name', 'name_product'
    ]].drop_duplicates()

    supplier_mapping = supplier_df.set_index('supplier_name')['id'].to_dict()
    order_details_df['supplier_id'] = order_details_df['supplier_name'].map(supplier_mapping)

    product_mapping = product_df.set_index('name_product')['id'].to_dict()
    order_details_df['product_id'] = order_details_df['name_product'].map(product_mapping)

    order_details_df['id'] = pd.RangeIndex(start=1, stop=len(order_details_df) + 1)

    return order_details_df[[
        'order_id', 'quantity', 'discount', 'return_flag', 'tax',
        'item_number', 'supplier_id', 'product_id'
    ]]


def handler_transform(df):
    """Función principal para transformar el DataFrame."""
    orders_df = normalize_orders(df)
    product_df = extract_product_data(df)
    customer_address_df, unique_nations = extract_customer_address_data(df)
    country_df = unique_nations.rename(columns={'name': 'customer_nation', 'id': 'country_id'})
    customer_address_df = map_customer_address_to_country(customer_address_df, country_df)
    customer_df = extract_customer_data(df, customer_address_df)
    time_df = extract_all_dates(df)
    supplier_df = extract_supplier_data(df)
    order_details_df = extract_order_details_data(df, supplier_df, product_df)

    # Aquí puedes retornar o imprimir los DataFrames finales según sea necesario
    print(orders_df.head())
    print(orders_df.columns)
    print()
    print(product_df.head())
    print(product_df.columns)
    print()
    print(customer_address_df.head())
    print(customer_address_df.columns)
    print()
    print(customer_df.head())
    print(customer_df.columns)
    print()
    print(time_df.head())
    print(time_df.columns)
    print()
    print(supplier_df.head())
    print(supplier_df.columns)
    print()
    print(order_details_df.head())
    print(order_details_df.columns)
    print()
    print(country_df.head())
    print(country_df.columns)

