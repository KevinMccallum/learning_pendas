import os
import sys
import pandas as pd
import numpy as np
from salesforcecallout import SalesforceConnection
from credentials import username, password, security_token

class DataHelper:
    def __init__(self):
        # self.file_folder = file_folder
        self.file = ''
        self.df = []

#|--------------------------------------------------------------------------------------------------|
#|----------------------------  DISTRICT ACCOUNT METHODS -------------------------------------------|
#|--------------------------------------------------------------------------------------------------|

    def readAndPrepareDistrictData(self, dc):
        dc = pd.DataFrame(dc)

        if 'superintendents' in self.file:
            dc['Sort'] = 1
            dc['Source'] = 'Superintendent File'
        elif 'district' in self.file:
            dc['Sort'] = 2
            dc['Source'] = 'District File'

        dc["Grades Offered - Lowest"] = np.where(dc['Grades Offered - Lowest'] == 'Kg', "K",(np.where(dc['Grades Offered - Lowest'] == 'KG',"K",np.where(dc['Grades Offered - Lowest'].str.startswith('0'), dc['Grades Offered - Lowest'].str.lstrip('0'),dc["Grades Offered - Lowest"]))))            
        dc["Grades Offered - Highest"].fillna(0)
        dc["Grades Offered - Highest"] = np.where(dc["Grades Offered - Highest"] == 'Kg', "K",(np.where(dc["Grades Offered - Highest"] == 'KG',"K",np.where((dc["Grades Offered - Highest"].isnull()) | dc["Grades Offered - Highest"].isna(),"",np.where(dc["Grades Offered - Highest"].astype(str).str.startswith('0'), dc["Grades Offered - Highest"].astype(str).str.lstrip('0'),dc["Grades Offered - Highest"])))))
        dc["Grades Offered - Highest"] = np.where(dc['Grades Offered - Highest'].astype(str).str.endswith('.0'),dc["Grades Offered - Highest"].astype(str).str.rstrip('.0'),dc["Grades Offered - Highest"])
        pd.to_numeric(dc["Grades Offered - Highest"], errors='coerce')
        dc["Grades Offered - Highest"] = np.where(dc["Grades Offered - Highest"].astype(str) == '13','12',dc["Grades Offered - Highest"])
        dc["Data Source Import Date"] = self.file.split(".xlsx")[0][-10:]
        # dc["Data Source Import Date"] = pd.to_datetime(dc["Data Source Import Date"],dayfirst=True)
        return dc

    def buildDistrictSchema(self, dc):
        schema = pd.DataFrame(dc)

        schema_to_return = schema[['District Id',
                                    'Full District Name',
                                    'County Name',
                                    'Location Address',
                                    'Location City',
                                    'Location State',
                                    'Location Zip',
                                    'Phone',
                                    'Total Number Of Students',
                                    'Teachers - Total',
                                    'Total Current Expenditures - Instruction',
                                    'Number Of Schools',
                                    'Instructional Expenditures Per Student',
                                    'Grades Offered - Lowest',
                                    'Grades Offered - Highest',
                                    'All Students - American Indian/alaska Native',
                                    'All Students - Asian',
                                    'All Students - Hispanic',
                                    'All Students - Black',
                                    'All Students - White',
                                    'All Students - Hawaiian Native / Pacific Islander',
                                    'All Students - Two Or More Races',
                                    'English Language Learner Students',
                                    'Source',
                                    'Sort',
                                    'Data Source Import Date']].copy()
        return schema_to_return
    
    def buildSchoolToDistrictSchema(self, dc):
        schema = pd.DataFrame(dc)
        schema_to_return = schema [['District Id',
                                    'District Name',
                                    'Nces School Id',
                                    'Full School Name',
                                    'School Name',
                                    'Location State',
                                    'County Name',
                                    'Source',
                                    'Sort',
                                    'Data Source Import Date']].copy()
        
        schema_to_return = schema_to_return[schema_to_return['District Name'].notnull()]
        
        schema_to_return["Full District Name"] = np.where(schema_to_return["District Name"].notnull(), schema_to_return['District Name'].astype(str).str.title() + ' (' + schema_to_return['Location State'] + ')',schema_to_return['Full School Name'].astype(str).str.title() + ' (' + schema_to_return['Location State'] + ')')
        return schema_to_return
    
    def populateDistrictDefaultValues(self, dc):
        accounts = pd.DataFrame(dc)
        accounts["Data_Source__c"] = "K12 - Prospects"
        accounts["Account Record Type"] = "012Du0000004KMfIAM"
        accounts["Record Type"] = "District"
        accounts["Account Type"] = "Unqualified"
        accounts["Public or Private"] = "Public"
        accounts["NCES_District_ID__c"] = accounts["District Id"]
        accounts["BillingStreet"] = accounts["Location Address"]
        accounts["BillingCity"] = accounts["Location City"]
        accounts["BillingState"] = accounts["Location State"]
        accounts["BillingPostalCode"] = accounts["Location Zip"]
        accounts['English Language Learner Students'] = np.where(accounts['English Language Learner Students'].notnull(), 'Yes', (np.where(accounts['English Language Learner Students'].isnull(),"","")))
        return accounts
    
    def changeDistrictAccountNameForDuplicateNames(self, dc):
        accounts = pd.DataFrame(dc)
        accounts["Temporary District Name"] = accounts["Full District Name"].astype(str).str.title() + ' (' + accounts['Location State'] + ' - ' + accounts['County Name'].astype(str).str.title() + ')'
        account_name = accounts["Full District Name"]
        duplicate_account_names = accounts[account_name.isin(account_name[account_name.duplicated()])].sort_values("Full District Name")
        
        if not duplicate_account_names.empty:
            duplicate_account_names["Full District Name"] = duplicate_account_names["Temporary District Name"]
            # duplicate_account_names["Sort"] = 1

            accounts = pd.concat([accounts, duplicate_account_names],ignore_index=False)
            accounts.sort_values(["Sort"], ascending=True, inplace=True)
            accounts.drop_duplicates(subset="District Id", keep='first', inplace=True)

        account_name = accounts["Full District Name"]
        duplicate_account_names = accounts[account_name.isin(account_name[account_name.duplicated()])].sort_values("Full District Name")


        if not duplicate_account_names.empty:
            emptydistrictaccounts = False
            while not emptydistrictaccounts:
                duplicate_account_names["Full District Name"] = duplicate_account_names["Temporary District Name"]+ ' - ' + duplicate_account_names['District Id'].astype(str)
                # duplicate_account_names["Sort"] = 1

                accounts = pd.concat([accounts, duplicate_account_names],ignore_index=False)
                accounts.sort_values(["Sort"], ascending=True, inplace=True)
                accounts.drop_duplicates(subset="District Id", keep='first', inplace=True)

                account_name = accounts["Full District Name"]
                duplicate_account_names = accounts[account_name.isin(account_name[account_name.duplicated()])].sort_values("Full District Name")

                if duplicate_account_names.empty:
                    emptydistrictaccounts = True
        return accounts
    
    
    def dedupeDistrictData(self,dc):
        deduped = pd.DataFrame(dc)
        deduped.sort_values(by="Sort", ascending=True,inplace=True)
        deduped.drop_duplicates(subset="District Id", keep='first', inplace=True)
        return deduped
    

    

    
#|--------------------------------------------------------------------------------------------------|
#|----------------------------  SCHOOL ACCOUNT METHODS ---------------------------------------------|
#|--------------------------------------------------------------------------------------------------|
    
    def dedupeSchoolData(self,dc):
        deduped = pd.DataFrame(dc)
        # deduped['Nces School Id'] = deduped["Nces School Id"].astype(np.int64)
        # deduped['Nces School Id'] = deduped.eval(deduped["Nces School Id"].astype(np.int64))
        deduped.sort_values(by="Sort", ascending=True,inplace=True)
        deduped.drop_duplicates(subset="Nces School Id", keep='first', inplace=True)
        return deduped
    
    def returnNonNumericalNcesSchoolId(self, dc):
        dc = pd.DataFrame(dc)
        dc = dc[~dc['Nces School Id'].apply(str).str.isnumeric()]
        return dc
    
    def returnNonNumericalNcesSchoolIdIndex(self, dc):
        dc = pd.DataFrame(dc)
        x = dc[~dc['Nces School Id'].apply(str).str.isnumeric()].index
        return x
    
    def dropRowbyIndex(self, dc, index):
        dc = pd.DataFrame(dc)
        dc.drop(labels=index, inplace=True)
        return dc
    

    def readAndPeparePublicSchoolData(self,dc):
        dc = pd.DataFrame(dc)  
        
        if 'public' in self.file:
            dc['Sort'] = 3
            dc['Source'] = 'Public File'
        if 'personnel' in self.file:
            dc['Sort'] = 4
            dc['Source'] = 'Personnel File'

        dc.loc[dc['Title I Eligible School'] == "Yes", 'Title I Eligible School'] = "True"
        dc.loc[dc['Title I Eligible School'] == "No", 'Title I Eligible School'] = "False"
        # dc['Nces School Id'] = dc['Nces School Id'].apply('="{}"'.format)
        # dc['Nces School Id2'] = dc['Nces School Id'].str.extract('((?=").*(?="))')
        # dc['Nces School Id2'].replace('"','',regex=True,inplace=True)
        dc["Grades Offered - Lowest"] = np.where(dc['Grades Offered - Lowest'] == 'Kg', "K",(np.where(dc['Grades Offered - Lowest'] == 'KG',"K",np.where(dc['Grades Offered - Lowest'].str.startswith('0'), dc['Grades Offered - Lowest'].str.lstrip('0'),dc["Grades Offered - Lowest"]))))

        
        dc["Grades Offered - Highest"].fillna(0)
        dc["Grades Offered - Highest"] = np.where(dc["Grades Offered - Highest"] == 'Kg', "K",(np.where(dc["Grades Offered - Highest"] == 'KG',"K",np.where((dc["Grades Offered - Highest"].isnull()) | dc["Grades Offered - Highest"].isna(),"",np.where(dc["Grades Offered - Highest"].astype(str).str.startswith('0'), dc["Grades Offered - Highest"].astype(str).str.lstrip('0'),dc["Grades Offered - Highest"])))))
        dc["Grades Offered - Highest"] = np.where(dc['Grades Offered - Highest'].astype(str).str.endswith('.0'),dc["Grades Offered - Highest"].astype(str).str.rstrip('.0'),dc["Grades Offered - Highest"])
        pd.to_numeric(dc["Grades Offered - Highest"], errors='coerce')
        dc["Grades Offered - Highest"] = np.where(dc["Grades Offered - Highest"].astype(str) == '13','12',dc["Grades Offered - Highest"])
        dc["Data Source Import Date"] = self.file.split(".xlsx")[0][-10:]


        # dc["Data Source Import Date"] = pd.to_datetime(dc["Data Source Import Date"],dayfirst=True)
        return dc
    
    
    def createAndAssignSchoolAccountName(self,dc):
        account = pd.DataFrame(dc)
        account["Account Name"] = np.where(account["Source"] == 'Public File', np.where((account["Full School Name"].notnull()) & (account['District Name'].notnull()), account['Full School Name'].astype(str).str.title() + ' - ' + account['District Name'].astype(str).str.title() + ' (' + account['Location State'] + ')',np.where(account["Full School Name"].isnull(),account['District Name'].astype(str).str.title() + ' (' + account['Location State'] + ')',account["Full School Name"].astype(str).str.title() + ' (' + account['Location State'] + ')')),
                                                             np.where(account["Source"] == 'Personnel File', np.where((account["School Name"].notnull()) & (account['District Name'].notnull()), account['School Name'].astype(str).str.title() + ' - ' + account['District Name'].astype(str).str.title() + ' (' + account['Location State'] + ')',np.where(account["School Name"].isnull(),account['District Name'].astype(str).str.title() + ' (' + account['Location State'] + ')',account["School Name"].astype(str).str.title() + ' (' + account['Location State'] + ')')),'null'))
        return account

    
    def buildSchoolSchema(self, dc):
        schema = pd.DataFrame(dc)
        schema_to_return = schema [['School Id',
                                    'Nces School Id',
                                    'School Name', 
                                    'District Name',
                                    'Full School Name',
                                    'District Id',
                                    'Phone',
                                    'Location Address',
                                    'Location City',
                                    'Location State',
                                    'Location Zip',
                                    'Grades Offered - Lowest',
                                    'Grades Offered - Highest',
                                    'Whether A Charter School',
                                    'Total Students, All Grades (includes Ae)',
                                    'Total Students',
                                    'County Name',
                                    'All Students - American Indian/alaska Native',
                                    'All Students - Asian',
                                    'All Students - Hispanic',
                                    'All Students - Black',
                                    'All Students - White',
                                    'All Students - Hawaiian Native / Pacific Islander',
                                    'All Students - Two Or More Races',
                                    'Classroom Teachers',
                                    'Title I Eligible School',
                                    'Source',
                                    'Sort',
                                    'Data Source Import Date']].copy()
        return schema_to_return
    

    
    def checkForNullSchoolAndFullSchoolName(self, dc):
        accounts = pd.DataFrame(dc)
        accounts_to_return = accounts[(accounts["School Name"].isnull()) & (accounts["Full School Name"].isnull())]
        accounts_to_return["Converted"] = 'Yes'
        return accounts_to_return


    
    def populateSchoolDefaultValues(self, dc):
        accounts = pd.DataFrame(dc)
        accounts["Data Source"] = "K12 - Prospects"
        accounts["Account Record Type"] = "012Du0000004KMzIAM"
        accounts["Record Type"] = "School"
        accounts["Account Type"] = "Unqualified"
        accounts["Public or Private"] = "Public"
        accounts["Street"] = accounts["Location Address"]
        accounts["City"] = accounts["Location City"]
        accounts["State"] = accounts["Location State"]
        accounts["PostalCode"] = accounts["Location Zip"]
        accounts["Data Source ID"] = accounts["School Id"]
        accounts['Total Students, All Grades (includes Ae)'] = accounts['Total Students']
        accounts.drop(['Total Students'],axis=1,inplace=True)
        return accounts
    
    
    def changeSchoolAccountNameForDuplicateNames(self, dc):
        accounts = pd.DataFrame(dc)

        accounts["Temporary Name"] = np.where(accounts["Source"] == "Public File", accounts["Full School Name"],accounts["School Name"])
        account_name = accounts["Account Name"]
        
        duplicate_account_names = accounts[account_name.isin(account_name[account_name.duplicated()])].sort_values("Account Name")

        if not duplicate_account_names.empty:
            duplicate_account_names["Account Name"] = np.where(duplicate_account_names["Source"] == 'Public File', np.where(pd.isnull(duplicate_account_names["Full School Name"]),duplicate_account_names['District Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')',np.where(pd.isnull(duplicate_account_names["District Name"]),duplicate_account_names['Full School Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')',duplicate_account_names['Full School Name'].astype(str).str.title() + ' - ' + duplicate_account_names['District Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')')),
                                                                np.where(duplicate_account_names["Source"] == 'Personnel File', np.where(pd.isnull(duplicate_account_names["School Name"]), duplicate_account_names['District Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')',np.where(pd.isnull(duplicate_account_names["District Name"]),duplicate_account_names['School Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')',duplicate_account_names['School Name'].astype(str).str.title() + ' - ' + duplicate_account_names['District Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')')),'null'))
            # duplicate_account_names["Sort"] = 1
            
            accounts = pd.concat([accounts, duplicate_account_names],ignore_index=False)
            accounts.sort_values(["Sort"], ascending=True, inplace=True)
            accounts.drop_duplicates(subset="Nces School Id", keep='first', inplace=True)
        
        account_name = accounts["Account Name"]
        duplicate_account_names = accounts[account_name.isin(account_name[account_name.duplicated()])].sort_values("Account Name")
        
        if not duplicate_account_names.empty:
            emptydistrictaccounts = False
            while not emptydistrictaccounts:
                duplicate_account_names["Account Name"] = np.where(duplicate_account_names["Source"] == 'Public File', np.where(pd.isnull(duplicate_account_names["Full School Name"]),duplicate_account_names['District Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')' + ' - ' + duplicate_account_names['Nces School Id'].astype(str),np.where(pd.isnull(duplicate_account_names["District Name"]),duplicate_account_names['Full School Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')' + ' - ' + duplicate_account_names['Nces School Id'].astype(str),duplicate_account_names['Full School Name'].astype(str).str.title() + ' - ' + duplicate_account_names['District Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')' + ' - ' + duplicate_account_names['Nces School Id'].astype(str))),
                                                                np.where(duplicate_account_names["Source"] == 'Personnel File', np.where(pd.isnull(duplicate_account_names["School Name"]), duplicate_account_names['District Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')' + ' - ' + duplicate_account_names['Nces School Id'].astype(str),np.where(pd.isnull(duplicate_account_names["District Name"]),duplicate_account_names['School Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')' + ' - ' + duplicate_account_names['Nces School Id'].astype(str),duplicate_account_names['School Name'].astype(str).str.title() + ' - ' + duplicate_account_names['District Name'].astype(str).str.title() + ' (' + duplicate_account_names['Location State'] + ' - ' + duplicate_account_names['Location City'].astype(str).str.title() + ')' + ' - ' + duplicate_account_names['Nces School Id'].astype(str))),'null'))
                # duplicate_account_names["Sort"] = 1
            
                # duplicate_account_names['Nces School Id'].apply(str)

                accounts = pd.concat([accounts, duplicate_account_names],ignore_index=False)
                accounts.sort_values(["Sort"], ascending=True, inplace=True)
                accounts.drop_duplicates(subset="Nces School Id", keep='first', inplace=True)

                account_name = accounts["Account Name"]
                duplicate_account_names = accounts[account_name.isin(account_name[account_name.duplicated()])].sort_values("Account Name")
                print(duplicate_account_names)
                if duplicate_account_names.empty:
                    emptydistrictaccounts = True
        return accounts
    

#|--------------------------------------------------------------------------------------------------|
#|---------------------------------  CONTACT METHODS -----------------------------------------------|
#|--------------------------------------------------------------------------------------------------|

    def dedupeContactData(self,dc):
        dc = pd.DataFrame(dc)
        dc["Email Address"] = dc["Email Address"].str.lower()
        dc = dc.sort_values(by="Sort", ascending=True).drop_duplicates(subset="Email Address", keep='first')
        return dc
    
    def removeAllDuplicatesBetweenK12andSalesforce(self,dc):
        dc = pd.DataFrame(dc)
        dc.drop_duplicates(subset="Email Address", inplace=True, keep=False)
        return dc
    
    def mergeContacts(self,dc,df):
        dc = pd.DataFrame(dc)
        df = pd.DataFrame(df)
        dataframe = dc.merge(df, on = ['Email Address'], how='right')
        return dataframe
    
    def populateContactDefaultValues(self,dc):
        dc = pd.DataFrame(dc)
        dc["Data Source"] = "K12 - Prospects"
        dc["Mailing Street"] = "1234 Penda Way"
        dc["Mailing Zip"] = 12340
        dc["Mailing City"] = "Sciencetown"
        return dc
    
    def buildExistingContactSchema(self,dc):
        dc = pd.DataFrame(dc)

        dc_to_return = dc [['ContactId',
                            'Account',
                            'Personnel Id',
                            'Nces School Id',
                            'District Id',
                            'First Name',
                            'Last Name',
                            'Title',
                            'Email Address',
                            'Phone',
                            'Location State',
                            'Sort',
                            'Source',
                            'Data Source Import Date']].copy()
        
        return dc_to_return

    
    #|---------------------------------  DISTRICT CONTACT METHODS -----------------------------------------------|
    
    def buildDistrictContactSchema(self,dc):
        dc = pd.DataFrame(dc)

        dc_to_return = dc [['District Id',
                            'Personnel Id',
                            'First Name',
                            'Last Name',
                            'Title',
                            'Email Address',
                            'Phone',
                            'Location State',
                            'Sort',
                            'Source',
                            'Data Source Import Date']].copy()
        
        return dc_to_return
    
    #|---------------------------------  SCHOOL CONTACT METHODS -----------------------------------------------|
    
    def buildSchoolContactSchema(self,dc):
        dc = pd.DataFrame(dc)

        dc_to_return = dc [['Personnel Id',
                            'Nces School Id',
                            'District Id',
                            'First Name',
                            'Last Name',
                            'Title',
                            'Email Address',
                            'Phone',
                            'Location State',
                            'Sort',
                            'Source',
                            'Data Source Import Date']].copy()
        
        return dc_to_return


#|----------------------------------------------------------------------------------------------------|
#|---------------------------------------  Salesforce ------------------------------------------------|
#|----------------------------------------------------------------------------------------------------|
    
    def queryForMarkForDeleteAndEmailOptOutContactsSFData(self,state):
        session = SalesforceConnection(username, password, security_token)
        response = session.connect().query_all('SELECT Id, Name, Email FROM Contact WHERE (Mark_for_Delete__c = true OR HasOptedOutOfEmail = true) AND MailingState = ' + "'" + state + "'")
        # print('MARK FOR DELETE CONTACT')
        # print(bool(response))
        # print(bool(response['records']))
        # print(response)
        if response['records']:
            df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
            df.rename(columns={'Email': 'Email Address'}, inplace=True)
            df["Source"] = 'Salesforce'
            return df
        else:
            return pd.DataFrame()
    
    def returnDuplicateContactsBetweenK12andSalesforce(self,dc):
        dc = pd.DataFrame(dc)
        dc = dc[dc["Email Address"].isin(dc["Email Address"][dc["Email Address"].duplicated()])].sort_values("Sort").drop_duplicates(subset="Email Address")
        return dc
    
    def queryAllContactsByState(self,state):
        session = SalesforceConnection(username, password, security_token)
        response = session.connect().query_all("SELECT Id, Name, Email, AccountId, Account.Id,Account.Type, Account.Name, Account.NCES_District_ID__c, Account.NCES_School_ID__c FROM Contact WHERE (Mark_for_Delete__c = false OR HasOptedOutOfEmail = false) AND (MailingState = " + "'" + state + "' OR Account.BillingState = " + "'" + state + "')")
        if response['records']:
            df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
            df.rename(columns={'Email': 'Email Address'}, inplace=True)
            df["Source"] = 'Salesforce'
            df['Sort'] = 5
            account_records = [dict(Id=rec['Account']['Id'], Type=rec['Account']['Type'], Name=rec['Account']['Name'], DistrictId=rec['Account']['NCES_District_ID__c'], SchoolId=rec['Account']['NCES_School_ID__c'])
           for rec in response['records']]
            account_records = pd.DataFrame(account_records)
            
            account_records.drop_duplicates(subset="Id", keep='first', inplace=True)
            account_records.rename(columns={'Id': 'AccountId', 'Name':'Account Name'}, inplace=True)

            df_to_return = account_records.merge(df, on = ['AccountId'], how='right')
            df_to_return.drop(labels='Account', axis=1, inplace=True)
            return df_to_return
        else:
            return pd.DataFrame()
    
    def queryForActiveProspectAndCurrentCustomerAccountwithContactsAndUnknownAccount(self,state):
        session = SalesforceConnection(username, password, security_token)
        response = session.connect().query_all("SELECT Id, Name, Email, AccountId, Account.Id,Account.Type, Account.Name, Account.NCES_District_ID__c, Account.NCES_School_ID__c FROM Contact WHERE ((MailingState = " + "'" + state + "' OR Account.BillingState = " + "'" + state + "') AND (Account.Type = 'Active Prospect' OR Account.Type LIKE 'Current Customer%')) OR Account.Name = 'Unknown'")
        if response['records']:
            df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
            df.rename(columns={'Email': 'Email Address'}, inplace=True)
            df["Source"] = 'Salesforce'
            df['Sort'] = 5

            account_records = [dict(Id=rec['Account']['Id'], Type=rec['Account']['Type'], Name=rec['Account']['Name'], DistrictId=rec['Account']['NCES_District_ID__c'], SchoolId=rec['Account']['NCES_School_ID__c'])
           for rec in response['records']]
            account_records = pd.DataFrame(account_records)
            
            account_records.drop_duplicates(subset="Id", keep='first', inplace=True)
            account_records.rename(columns={'Id': 'AccountId', 'Name':'Account Name'}, inplace=True)

            df_to_return = account_records.merge(df, on = ['AccountId'], how='right')
            df_to_return.drop(labels='Account', axis=1, inplace=True)
            return df_to_return
        else:
            return pd.DataFrame()
    
    def returnContactsFromQuery(self, dc):
        dc = pd.DataFrame(dc)
        dc = dc['Contacts'].apply(lambda x:x['records'])
        contact_list_df = pd.DataFrame()

        for i in range(0,len(dc)): 
            contact = dc[i]
            contact_new_df = pd.DataFrame(contact)
            contact_list_df = pd.concat([contact_list_df,contact_new_df], ignore_index=True)

        contact_list_df.drop(labels='attributes', axis=1, inplace=True)
        contact_list_df.rename(columns={'Email': 'Email Address'}, inplace=True)

        return contact_list_df
    
    def queryContactHistory(self):
        session = SalesforceConnection(username, password, security_token)
        response = session.connect().query_all('SELECT ContactId,Contact.Id, Contact.Email, Contact.Name, Contact.Account.Id, OldValue, NewValue, Id, Field,FORMAT(CreatedDate) From ContactHistory WHERE Field = \'Account_NCES_ID__c\' AND DataType != \'EntityId\' AND (CreatedDate = THIS_QUARTER OR CreatedDate = LAST_QUARTER) ') #AND Contact.MailingState = ' + "'" + state + "'"
        if response['records']:
            df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
            df["Source"] = 'Salesforce'
            df['Sort'] = 5
            return df
        else:
            return pd.DataFrame()
        
    def returnContactsFromContactHistoryQuery(self, dc):
        dc = pd.DataFrame(dc)
        contact_df = dc['Contact']
        contact_list_df = pd.DataFrame()

        for i in range(0,len(contact_df)): 
            contact = contact_df[i]
            contact_new_df = pd.DataFrame(contact).loc[['Id']]
            contact_list_df = pd.concat([contact_list_df,contact_new_df], ignore_index=True)

        contact_list_df.drop(labels='attributes', axis=1, inplace=True)
        contact_list_df.rename(columns={'Email': 'Email Address'}, inplace=True)
        contact_list_df.rename(columns={'Id': 'ContactId'}, inplace=True)

        return contact_list_df
    
    def queryDistrictAccountsByState(self, state):
        session = SalesforceConnection(username, password, security_token)
        response = session.connect().query_all("SELECT Id, Name, NCES_District_ID__c, Type FROM Account WHERE BillingState = '" + state +"' AND District_or_School_Record__c = 'District' ")
        if response['records']:
            df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
            df.rename(columns={'NCES_District_ID__c': 'District Id', 'Name':'Account Name'}, inplace=True)
            return df
        else:
            return pd.DataFrame()
        
    def querySchoolAccountsByState(self, state):
        session = SalesforceConnection(username, password, security_token)
        response = session.connect().query_all("SELECT Id, Name, NCES_School_ID__c, Type FROM Account WHERE BillingState = '" + state +"' AND District_or_School_Record__c = 'School' ")
        if response['records']:
            df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
            df.rename(columns={'Name':'Account Name', 'NCES_School_ID__c':'Nces School Id'}, inplace=True)
            return df
        else:
            return pd.DataFrame()