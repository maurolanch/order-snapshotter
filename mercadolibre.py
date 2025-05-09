import requests
from google.cloud import secretmanager

def get_secret(secret_id, version_id="latest", project_id="lanch-pipeline-v3"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def get_order_snapshot(order_id):
    access_token = get_secret("ML_ACCESS_TOKEN_CUENTA1")  
    url = f"https://api.mercadolibre.com/orders/{order_id}"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Error al obtener la orden: {response.status_code} - {response.text}")
    
    return response.json()
