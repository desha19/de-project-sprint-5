import requests

nickname = 'deb'
cohort = '25'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

headers = {"X-Nickname" : nickname,
         'X-Cohort' : cohort,
         'X-API-KEY' : api_key,

         }

start  = "2024-01-01 05:33:24"
end    =  "2024-06-14 05:35:24"
offset = 0
limit = 10
sort_field = 'order_ts'
sort_direction = 'asc'
#restaurant_id = 'ef8c42c19b7518a9aebec106'

r = requests.get(f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?from={start}&to={end}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}',
                            headers = headers).json()
                            

print(r)