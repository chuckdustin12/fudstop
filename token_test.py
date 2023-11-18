import requests
import json
headers = {
    'Content-Type': 'application/json',
  
}

payload = {
    "clientKey":"ioleKyXAcbAGCK6mnGE2oxo7KuPtDEbP",
    "clientName": "OIC_test"
}
r = requests.post(
    "https://ivlivefront.ivolatility.com/token/client/get",
    headers=headers,
    json=payload
)

print(r.text)