import  datetime
obj = {'_id': {'begin_date': '2020-11-16T00:00:00.000+00:00', 'end_date': '2020-11-16T00:00:00.000+00:00', 'country_code': 'AE'}, 'reports': [{'provider': 'APPLE', 'provider_country': 'US', 'sku': 'NDR', 'developer': 'Feeld Ltd', 'title': 'Feeld: For Couples & Singles', 'version': '5.8.19', 'product_type_identifier': '1', 'units': 1, 'developer_proceeds': 0.0, 'begin_date': '2020-11-16T00:00:00.000+00:00', 'end_date': '2020-11-16T00:00:00.000+00:00', 'customer_currency': 'AED', 'country_code': 'AE', 'currency_of_proceeds': ' ', 'apple_identifier': 887914690, 'customer_price': 0.0, 'promo_code': ' ', 'parent_identifier': ' ', 'subscription': ' ', 'period': ' ', 'category': 'Lifestyle', 'cmb': '', 'device': 'iPhone', 'supported_platforms': 'iOS', 'proceeds_reason': ' ', 'preserved_pricing': ' ', 'client': ' ', 'order_type': ' '}, {'provider': 'APPLE', 'provider_country': 'US', 'sku': 'NDR', 'developer': 'Feeld Ltd', 'title': 'Feeld: For Couples & Singles', 'version': '5.8.19', 'product_type_identifier': '3', 'units': 8, 'developer_proceeds': 0.0, 'begin_date': '2020-11-16T00:00:00.000+00:00', 'end_date': '2020-11-16T00:00:00.000+00:00', 'customer_currency': 'AED', 'country_code': 'AE', 'currency_of_proceeds': 'AED', 'apple_identifier': 887914690, 'customer_price': 0.0, 'promo_code': ' ', 'parent_identifier': ' ', 'subscription': ' ', 'period': ' ', 'category': 'Lifestyle', 'cmb': '', 'device': 'iPhone', 'supported_platforms': 'iOS', 'proceeds_reason': ' ', 'preserved_pricing': ' ', 'client': ' ', 'order_type': ' '}, {'provider': 'APPLE', 'provider_country': 'US', 'sku': 'NDR', 'developer': 'Feeld Ltd', 'title': 'Feeld: For Couples & Singles', 'version': '5.4.2', 'product_type_identifier': '3', 'units': 1, 'developer_proceeds': 0.0, 'begin_date': '2020-11-16T00:00:00.000+00:00', 'end_date': '2020-11-16T00:00:00.000+00:00', 'customer_currency': 'AED', 'country_code': 'AE', 'currency_of_proceeds': 'AED', 'apple_identifier': 887914690, 'customer_price': 0.0, 'promo_code': ' ', 'parent_identifier': ' ', 'subscription': ' ', 'period': ' ', 'category': 'Lifestyle', 'cmb': '', 'device': 'iPhone', 'supported_platforms': 'iOS', 'proceeds_reason': ' ', 'preserved_pricing': ' ', 'client': ' ', 'order_type': ' '}, {'provider': 'APPLE', 'provider_country': 'US', 'sku': 'NDR', 'developer': 'Feeld Ltd', 'title': 'Feeld: For Couples & Singles', 'version': '5.8.19', 'product_type_identifier': '7', 'units': 1, 'developer_proceeds': 0.0, 'begin_date': '2020-11-16T00:00:00.000+00:00', 'end_date': '2020-11-16T00:00:00.000+00:00', 'customer_currency': 'AED', 'country_code': 'AE', 'currency_of_proceeds': 'AED', 'apple_identifier': 887914690, 'customer_price': 0.0, 'promo_code': ' ', 'parent_identifier': ' ', 'subscription': ' ', 'period': ' ', 'category': 'Lifestyle', 'cmb': '', 'device': 'iPhone', 'supported_platforms': 'iOS', 'proceeds_reason': ' ', 'preserved_pricing': ' ', 'client': ' ', 'order_type': ' '}], 'daily': True, 'created_at': '2020-11-18T09:10:04.626+00:00'}

print(obj)
print(type(obj))

alerts = obj['begin_date']
obj['begin_date'] = datetime.strptime(obj['begin_date'], "%Y-%m-%d %H:%M:%S")