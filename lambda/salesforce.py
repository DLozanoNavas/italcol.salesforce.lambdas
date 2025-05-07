import requests

def authenticate(url, params):
  response = requests.post(url=url, params=params, headers={"Content-Type": "application/json"})

  if response.status_code == 200:
    token_data = response.json()
    return {
      "access_token" : token_data.get("access_token"),
      "base_url" : token_data.get("instance_url")
    }
  raise Exception(f"Authentication failed: {response.text}")

