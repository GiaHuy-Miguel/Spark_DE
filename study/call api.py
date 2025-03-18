import requests

data = requests.get("https://jsonplaceholder.typicode.com/todos/1")

print(data.text)