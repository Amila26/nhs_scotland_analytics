import requests
import msal
from datetime import datetime
from myscript.my_cred import username,password
from myscript.carn_API_data import create_log_file
def get_token():

    app_id = "add_your_app_id_from_Azure_app_registration"
    tenant_id = "add_your_tenat_id_from_Azure_app_registration"


    authority_url = 'https://login.microsoftonline.com/' + tenant_id
    scope = ['https://analysis.windows.net/powerbi/api/.default']

    # generate Access Token
    client = msal.PublicClientApplication(app_id,authority = authority_url)
    response = client.acquire_token_by_username_password(username=username,password=password,scopes=scope)
    create_log_file(str(response.get('token_type')) + "PBI token status")

    access_id = response.get('access_token')
    return access_id


# Call Simple API
def refresh_data():
    datasetId = "7e828b2d-8fbf-44d2-a5b1-cdbdb8bf9159"
    groupId = "bc749863-3c0a-4d63-b0f8-c3cba464f718"
    endpoint =  f"https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes"

    token = get_token()

    headers = {
        'Authorization': f'Bearer ' + token
    }
    response_request = requests.post(endpoint,headers=headers)
    create_log_file(f'refresh status is {response_request.status_code} and reason {response_request.headers}')

    with open("/opt/airflow/windows-docs/refresh_time.csv","a") as file:
        if response_request.status_code == 202:
            file.write(str(datetime.now().isoformat()) + "," + "refresh_success"+"\n")
        else:
            file.write(str(datetime.now().isoformat()) + "," + "refresh_failed"+"\n")
    
    create_log_file("...............ETL finished............." +"\n"+"\n")
    

    

   

# Update_time_in_data Set

def realtime_data():
    token = get_token()
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'  # Set content-type header
    }
    dataset_endpoint = "https://api.powerbi.com/beta/91f0e2c8-bb29-4788-ba88-85ed017ee3b3/datasets/ac625fc1-46fa-4bae-ad97-5466508288d7/rows?experience=power-bi&key=Qnw7O4U11AdQhyF8UADBuVuN7d1zuZNEvTXNRn1vbESF%2FtrEWE%2F8A8LJuzVPstnaW%2F1nWB9GlkxJ%2F9Tt8TGEzQ%3D%3D"

    body = {
        "rows": [
            {
                "UpdateTime": datetime.now().isoformat()
            }
        ]
    }


    response_request = requests.post(dataset_endpoint, headers=headers, json=body)
    print(f'data Update status is {response_request.status_code} and reason {response_request.reason}')


# refresh_data()
# #realtime_data()
# get_token()
