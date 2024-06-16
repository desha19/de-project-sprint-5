import requests

nickname = 'deb'
cohort = '25'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

headers = {"X-Nickname" : nickname,
         'X-Cohort' : cohort,
         'X-API-KEY' : api_key
         }

offset  =  0
limit = 5
sort_field = '_id'
sort_direction = 'asc'

r = requests.get(f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}',
                            headers = headers).json()

print(r)
