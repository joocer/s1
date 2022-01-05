from fastapi import responses
import requests

response = requests.get("http://localhost:8080/gva?location=")

print(response.headers)
print(response.content.decode())

response = requests.get("http://localhost:8080/gva?delimiter=")

print(response.headers)
print(response.content.decode())

response = requests.get("http://localhost:8080/gva/qvm/reports/data.xml")

print(response.headers)
print(response.content.decode())