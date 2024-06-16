import requests

nickname = 'deb'
cohort = '25'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

headers = {"X-Nickname" : nickname,
         'X-Cohort' : cohort,
         'X-API-KEY' : api_key
         }

#start  = "2024-05-04 05:33:24"
#end     =  "2024-05-04 05:35:24"
offset  =  0
limit = 20
r = requests.get(f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants/?sort_field=id&sort_direction=asc&offset={offset}&limit={limit}',
                            headers = headers).json()

print(r)
