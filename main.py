import ET_Client
import pandas as pd
import numpy as np
import pandas_gbq
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import Forbidden
from google.oauth2 import service_account

try:
    debug = False
    #update stubObj with your sfmc credentials
    stubObj = ET_Client.ET_Client(False, False, {})
    
    #gcloud creds
    credentials = service_account.Credentials.from_service_account_info(
    {},
)
    NameOfDE = "All Subscribers Export"
    Emails_list = []
    Status_list = []
    SubscriberKey_list = []
    

    
    # Retrieve lots of rows with moreResults
    print('>>> Retrieve lots of rows with moreResults')
    row = ET_Client.ET_DataExtension_Row()
    row.auth_stub = stubObj
    row.Name = "All Subscribers Export"
    row.props = ["Email Address","Status","Subscriber Key"]
    getResponse = row.get()
    print('Retrieve Status: ' + str(getResponse.status))
    #print('Code: ' + str(getResponse.code))
    #print('Message: ' + str(getResponse.message))
    #print('MoreResults: ' + str(getResponse.more_results))
    #print('RequestID: ' + str(getResponse.request_id))
    print('Results Length: ' + str(len(getResponse.results)))
    #print ('Results: ' + str(getResponse.results))
    rowCount = len(getResponse.results)
    for a in range(0,rowCount):
        EmailAddress = str(getResponse.results[a]["Properties"][0][0]["Value"])
        Status = str(getResponse.results[a]["Properties"][0][1]["Value"])
        SubscriberKey = str(getResponse.results[a]["Properties"][0][2]["Value"])
        Emails_list.append(EmailAddress)
        SubscriberKey_list.append(SubscriberKey)
        Status_list.append(Status)

    while getResponse.more_results: 
        print('>>> Continue Retrieve lots of rows with moreResults')
        getResponse = row.getMoreResults()
        print('Retrieve Status: ' + str(getResponse.status))
        #print('Code: ' + str(getResponse.code))
        #print('Message: ' + str(getResponse.message))
        #print('MoreResults: ' + str(getResponse.more_results))
        #print('RequestID: ' + str(getResponse.request_id))
        print('Results Length: ' + str(len(getResponse.results)))
        rowCount = len(getResponse.results)
        for z in range(0,rowCount):
            EmailAddress = str(getResponse.results[z]["Properties"][0][0]["Value"])
            Status = str(getResponse.results[z]["Properties"][0][1]["Value"])
            SubscriberKey = str(getResponse.results[z]["Properties"][0][2]["Value"])
            Emails_list.append(EmailAddress)
            SubscriberKey_list.append(SubscriberKey)
            Status_list.append(Status)
        

    Combined_list = pd.DataFrame(np.column_stack([Emails_list, SubscriberKey_list, Status_list]), 
                               columns=['Emails', 'SubscriberKey', 'Status'])
    
    destination_table = 'yourproject.yourtable'
    project = 'yourproject'
    Combined_list.to_gbq(destination_table, project_id=project, chunksize=None, reauth=False, if_exists='replace', auth_local_webserver=False, table_schema=None, location=None, progress_bar=True, credentials=credentials)

    print("done.. hoorah!")
            
except Exception as e:
    print('Caught exception: ' + str(e.message))
    print(e)
