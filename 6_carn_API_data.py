import requests
import pandas as pd
import os
from datetime import datetime
import time
def request_fn():
    data_list = []
    end_point = "https://www.opendata.nhs.scot/api/3/action/datastore_search"
    data_set_id = "df65826d-0017-455b-b312-828e47df325b"

    payload = {
        "resource_id":data_set_id,
        'limit': 50,
    }

    # dict_keys(['help', 'success', 'result'])
    # dict_keys(['include_total', 'resource_id', 'fields', 'records_format', 'records', '_links', 'total'])
    try :
        response = requests.get(url=end_point,params=payload,timeout=10)
        if response.status_code == 200:
            print("first request is sucess ")
            print(response.headers['Content-Type'])
            create_log_file("request_succesful ->" + str(response.status_code)) # Initial Request Done

            # extract all content in 1st request
            all_content = response.json()
            #print(all_content.keys())
            create_log_file("response_keys" + str(all_content.keys()))
            #print(all_content.get('result').keys())
            create_log_file("response_result" + str(all_content.get('result').keys()))
            records = all_content.get('result',{}).get('records',[])
            data_list.append(records)
            #print("This is data list",len(records))
            create_log_file("This is data list" + str(len(records)))


            links = all_content.get('result',{}).get('_links',[])['next']
            paginated_fn(links,data_list) # Handle paginate

    except Exception as e:
        print(e)
        create_log_file("error ->" + str(e)) # Record Error To Log File
        return
    return data_list

def file_path():
    if __name__ =='__main__':
        path_file = "/mnt/d/airflow/airflow_logs/nhs_data_file.csv"
    else:
        path_file = "/opt/airflow/windows-docs/nhs_data_file.csv"
    return path_file

def paginated_fn(pagilink,data_for_append):
    base_url = "https://www.opendata.nhs.scot"
    paginate_link = base_url+pagilink
    try:
        next_response = requests.get(paginate_link,timeout=20) # get
        pagi_records = next_response.json().get('result',{}).get('records',[])
        
        if len(pagi_records)> 0:
            data_for_append.append(pagi_records)
            #print("append_next_page",len(pagi_records))
            create_log_file("append_next_page" + str(len(pagi_records)))
            
            paginate_link = next_response.json().get('result',{}).get('_links',[])['next']
            #time.sleep(30)
            paginated_fn(paginate_link,data_for_append)
        else:
            #print("paginate_finished")
            #print(len(data_for_append))
            #print( data_for_append)
            create_log_file("Paginate_finished /Total Records" + str(len(data_for_append)))

    except Exception as e:
        #print("paginate link error") 
        create_log_file("paginate link error" + str(e))
        print(e)
        return

def save_data():
    extract_data_api = request_fn()
    if extract_data_api is not None and len(extract_data_api)>0:
        flattend_list=[]
        for eliment in extract_data_api:
            for x in eliment:
                flattend_list.append(x)
        print(len(flattend_list)) 
        my_df = pd.DataFrame(flattend_list)
        save_path = file_path()
        my_df.to_csv(save_path)
        #print("save and update data in repository")
        create_log_file("save and update data in repository")

    else:
        #print('No Data from extract API')
        create_log_file("No Data from extract API")  


def check_file():
    try:
        path_file = file_path()
        if os.path.isfile(path_file) and os.path.getsize(path_file)>2 * 1024: # dont use & which is bitwise operator 2*1024 =2kb
            availability = "yes"
        else:
            availability = "no"
    except Exception as e:
        print(e)
        return
    return availability

def  data_transform():
    file_name = file_path()
    nhs_df = pd.read_csv(file_name,index_col=False)
    nhs_df =nhs_df.iloc[:,2:]
    nhs_df["Year"] = nhs_df["Month"].astype(str).str[:4]
    nhs_df["Month_no"] = nhs_df["Month"].astype(str).str[4:]
    nhs_df = nhs_df.melt(
        id_vars=['Month', 'Country','Year', 'Month_no'],
        value_vars=['TotalOperations', 'TotalOperationsQF',
       'TotalCancelled', 'TotalCancelledQF', 'CancelledByPatientReason',
       'CancelledByPatientReasonQF', 'ClinicalReason', 'ClinicalReasonQF',
       'NonClinicalCapacityReason', 'NonClinicalCapacityReasonQF',
       'OtherReason', 'OtherReasonQF', 'UnknownReason', 'UnknownReasonQF'],
       var_name="Reason_category",
       value_name="Value"
       )
    nhs_df['Value'] = nhs_df['Value'].fillna(0)
    print(nhs_df.info())
    nhs_df.to_csv(os.path.split(file_name)[0] +"/nhs_transform_data.csv",index_label="id")
    create_log_file("Data Transformd") 

def create_log_file(msg):
    file_name = file_path()
    path_name = os.path.split(file_name)[0] + "/nhs_log.csv"
    with open(path_name,"a") as file:
        file.write(datetime.now().isoformat(timespec='seconds') +" \u2192 " + msg +"\n" )



if __name__ =='__main__':
    save_data()
    data_transform()
    # create_log_file("ETL Started")
    # request_fn()


# Extract Logic as follows 
# define data_lits [] empty list withing the function request
# made first request 
# extarct data,links,paginate data pass function 
# mutate data_list and return to save function 
# Save if main in physical file but if in airflow it should in mounted file
# draw backs if not pagiante some time can be error
