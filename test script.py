import requests

baseUrl = "http://0.0.0.0:8085/key-value-store-view"
response = requests.get(baseUrl)

print(response)
