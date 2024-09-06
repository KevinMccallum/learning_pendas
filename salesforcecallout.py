import pandas as pd
from simple_salesforce import Salesforce


class SalesforceConnection:
    def __init__(self, username, password, security_token):
        self.username = username
        self.password = password
        self.security_token = security_token
        

    def connect(self):
        return Salesforce(username=self.username,password=self.password,security_token=self.security_token,organizationId='00D6g000008Lv63')
    

# login = SalesforceConnection.connect()
# response = SalesforceConnection.connect().query('SELECT Id FROM Account LIMIT 1')

# df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
# print(df)
# sf_instance = 'https://learning2020--kevin.sandbox.my.salesforce.com'
# reportId = '00OVA00000017HB2AY'
# export = '?isdtp=pl&export=1&enc=UTF-8&xf=csv'
# sfURL = sf_instance + reportId + export
# response = requests.get(sfURL,headers=sf.headers,cookies={'sid':sf.session_id})
# download_report = response.content.decode('utf-8')
# df1 = pandas.read_csv(download_report)

# print(df1)

# https://learning2020--kevin.sandbox.my.salesforce.com00OVA00000017HB2AY?isdtp=pl&export=1&enc=UTF-8&xf=csv