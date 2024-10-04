from flask import Flask, render_template, request, send_file, redirect, url_for,make_response,session
from flask_session import Session
import pandas as pd
import numpy as np
import requests
# from simple_salesforce import Salesforce
from salesforcecallout import SalesforceConnection
from credentials import username, password, security_token
from dedupek12 import DataHelper
# from io import StringIO
import os
from datetime import date




today = date.today()

# session = SalesforceConnection(username, password, security_token)

# response = session.connect().query('SELECT Id FROM Account LIMIT 1')
# # response = requests.get(sfURL,headers=sf.headers,cookies={'sid':sf.session_id})
# # download_report = response.content.decode('utf-8')
# # df1 = pandas.read_csv()

# df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
# print(df)

# success = open("templates/success.html").read().format(records=df)


app=Flask(__name__)
app.secret_key = 'super secret key'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/success-table", methods=['POST'])
def success_table():
    # response = request.form['file']
    school = DataHelper()
    salesforce_session_connection = SalesforceConnection(username, password, security_token)
    response = salesforce_session_connection.connect().query_all("SELECT Id, Name, Email, AccountId, Dont_Edit__c FROM Contact WHERE (MailingState = 'FL' OR Account.BillingState = 'FL')")
    # response = school.queryContactHistoryAccountId()
    df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
    # contact_df = df['Contact'].apply(lambda x:x['Contact'])
    # contact_df = pd.DataFrame(df['Contact'])
    # account_df = pd.DataFrame(df['Account'])

    # records = [dict(Id=rec['Account']['Id'], Type=rec['Account']['Type'], Name=rec['Account']['Name'])
    #        for rec in response['records']]


    


    
    # account_df = pd.DataFrame(df).drop(labels='attributes', axis=1)


    # df.rename(columns={'Email': 'Email Address'}, inplace=True)
    # df["Source"] = 'Salesforce'
    # df['Sort'] = 5
    # account_records = [dict(ContactId=['Id'],Name=['Name'],Email=['Email'],AccountId=rec['Account']['Id'], Type=rec['Account']['Type'], AccountName=rec['Account']['Name'], DistrictId=rec['Account']['NCES_District_ID__c'], SchoolId=rec['Account']['NCES_School_ID__c'])
    #        for rec in response['records']]
    # account_records = pd.DataFrame(account_records)




    # contact_list_df = pd.DataFrame()

    # for i in range(0,len(contact_df)): 
    #     contact = contact_df[i]
    #     contact_new_df = pd.DataFrame(contact)#.loc[['Id']]
    #     contact_list_df = pd.concat([contact_list_df,contact_new_df], ignore_index=True)

    # contact_list_df.drop(labels='attributes', axis=1, inplace=True)
    # contact_new_df.drop(labels='attributes', axis=1, inplace=True)
    # print(contact_new_df)

    
    # contact_list_df.to_csv(f'contacts_df{today}.csv', index=False, float_format='%.0f')
    # df.to_csv(f'sf_accounts_df{today}.csv', index=False, float_format='%.0f')
    # salesforce_session = Salesforce(username, password, security_token)

    # report_id = '00OVA0000005mYf2AI' # However you're obtaining your Report Id
    # report_results = salesforce_session.restful('analytics/reports/{}'.format(report_id))
    # print(report_results)

    return render_template('success.html',  tables=[df.to_html(classes='data', index=True, justify='center', index_names=True)], titles=df.columns.values)

@app.route("/dedupe-k12", methods=["POST"])
def dedupek12():

    district_df = []
    school_df = []

    response = request.form['file']
    state = request.form['state']

    district = DataHelper()
    school = DataHelper()
    #Pike Preparatory Academy
    #1808910N0048

    for file in os.listdir(response):
        dc = pd.read_excel(os.path.join(response,file),dtype={'Nces School Id': str, 'District Id': str, 'Grades Offered - Lowest': str})
        dc['District Id'] = np.trim_zeros(dc['District Id'],'f')
        dc['District Id'] = np.where(dc['District Id'].str.startswith('0'), dc['District Id'].str.lstrip('0'),dc['District Id'])
        # print(response)
        dc["Data Source Import Date"] = response.split(".xlsx")[0][-10:]
        if 'superintendents' in file and file.endswith('.xlsx'):
            dc["Data Source Import Date"] = pd.to_datetime(dc["Data Source Import Date"],dayfirst=True)
            district.file = file
            district_df.append(district.readAndPrepareDistrictData(dc))
        elif 'district' in file and file.endswith('.xlsx'): 
            dc["Data Source Import Date"] = pd.to_datetime(dc["Data Source Import Date"],dayfirst=True)  
            district.file = file
            district_df.append(district.readAndPrepareDistrictData(dc))

        elif 'public' in file and file.endswith('.xlsx'):
            school.file = file
            school_df.append(school.readAndPeparePublicSchoolData(dc))

        elif 'school_personnel' in file and file.endswith('.xlsx'):
            school.file = file        
            school_df.append(school.readAndPeparePublicSchoolData(dc))

  
    district_df = pd.concat(district_df, axis=0,ignore_index=False)
    school_df = pd.concat(school_df,axis=0,ignore_index=False)

    


#|--------------------------------------------------------------------------------------------------|
#|--------------------------------------------  ACCOUNTS -------------------------------------------|
#|--------------------------------------------------------------------------------------------------|

    #Create Schema For Schools that don't have a School Name and Full School Name
    schoolsToCheckIfDistrict = school.buildSchoolToDistrictSchema(school_df)
    

    #Check for Schools that don't have  a School Name and Full School Name
    schoolsToMergeAsDistricts = school.checkForNullSchoolAndFullSchoolName(schoolsToCheckIfDistrict)
    # schoolsToMergeAsDistricts.to_csv(f'schoolsToMergeAsDistricts{today}.csv', index=False, float_format='%.0f')

    #Create Schema for District and School Accounts
    main_district_df = district.buildDistrictSchema(district_df)
    main_school_df = school.buildSchoolSchema(school_df)

    #Concatenate Schools without School Name and Full School Name to District DataFrame
    if not schoolsToMergeAsDistricts.empty:
        main_district_df = pd.concat([main_district_df, schoolsToMergeAsDistricts], axis=0,ignore_index=False)
        main_district_df = district.buildDistrictSchema(district_df)

    #Dedupe District accounts by District Id and School accounts by NCES School Id
    deduped_main_district_df = district.dedupeDistrictData(main_district_df)
    deduped_main_district_df = district.setDistrictNamewithState(deduped_main_district_df)



    #Check if there is Non-numerical Nces School Id
    non_numerical_nces_school_id = school.returnNonNumericalNcesSchoolId(main_school_df)

    if not non_numerical_nces_school_id.empty:

        non_numerical_nces_school_id_index = school.returnNonNumericalNcesSchoolIdIndex(main_school_df)
        numerical_main_school_df = school.dropRowbyIndex(main_school_df,non_numerical_nces_school_id_index)
        deduped_main_school_df = school.dedupeSchoolData(numerical_main_school_df)
        deduped_main_school_df = pd.concat([deduped_main_school_df,non_numerical_nces_school_id], axis=0,ignore_index=False)

    else:

        deduped_main_school_df = school.dedupeSchoolData(main_school_df)

      

    #Create "Account Name" column and populate it depending on the file to use School Name or Full School Name
    deduped_main_school_df = school.createAndAssignSchoolAccountName(deduped_main_school_df)

    #Populate District and School Accounts with their corresponding Default Values for SF import
    deduped_main_district_df = district.populateDistrictDefaultValues(deduped_main_district_df)
    deduped_main_school_df = school.populateSchoolDefaultValues(deduped_main_school_df)
    
    #Check for District and School Accounts with the same name and update their name with State, County and Id if necessary
    deduped_main_district_df = district.changeDistrictAccountNameForDuplicateNames(deduped_main_district_df)
    deduped_main_school_df = school.changeSchoolAccountNameForDuplicateNames(deduped_main_school_df)
    
    deduped_main_school_df.reset_index(inplace=True)
    
    
    deduped_main_school_df.to_csv(f'{state} - school_df_organized{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    deduped_main_district_df.to_csv(f'{state} - district_df_organized{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')


# #|----------------------------------------------------------------------------------------------------|
# #|------------------------------  Salesforce DISTRICT ACCOUNTS ---------------------------------------|
# #|----------------------------------------------------------------------------------------------------|
            

    district_accounts_by_state = district.queryDistrictAccountsByState(state)

    if not district_accounts_by_state.empty:
        district_accounts_by_state.to_csv(f'{state} - District Salesforce Accounts{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

        merged_district_accounts_with_salesforce_data = deduped_main_district_df.merge(district_accounts_by_state, on =['District Id'],how='outer', indicator=True)
        district_accounts_not_in_salesforce = merged_district_accounts_with_salesforce_data[merged_district_accounts_with_salesforce_data['_merge'] == 'left_only']
        district_both = merged_district_accounts_with_salesforce_data[merged_district_accounts_with_salesforce_data['_merge'] == 'both']
        district_right = merged_district_accounts_with_salesforce_data[merged_district_accounts_with_salesforce_data['_merge'] == 'right_only']
        district_accounts_not_in_salesforce.drop(labels={'Id', 'Account Name','Type','_merge'}, axis=1, inplace=True)
        district_accounts_to_upsert = pd.concat([district_accounts_not_in_salesforce, district_both], axis=0,ignore_index=False)
        district_accounts_to_upsert = district_accounts_to_upsert[district_accounts_to_upsert['Dont_Edit__c'] == False]

        # district_accounts_not_in_salesforce(subset="District Id", keep='first', inplace=True)
        district_accounts_not_in_salesforce.to_csv(f'{state} - District Accounts Not In Salesforce{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        # district_accounts_to_upsert(subset="District Id", keep='first', inplace=True)
        district_accounts_to_upsert.to_csv(f'{state} - District Accounts To Upsert{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        # district_right.to_csv(f'{state} - district_right{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    

#|----------------------------------------------------------------------------------------------------|
#|---------------------------------------------  CONTACTS --------------------------------------------|
#|----------------------------------------------------------------------------------------------------|

    district_df["Data Source Import Date"] = district_df["Data Source Import Date"].astype('int64') 
    main_contact_district_df = district.buildDistrictContactSchema(district_df)
    main_contact_school_df = school.buildSchoolContactSchema(school_df)


    deduped_main_contact_district_df = district.dedupeContactData(main_contact_district_df)
    deduped_main_contact_school_df = school.dedupeContactData(main_contact_school_df)


    deduped_main_contact_school_df = deduped_main_contact_school_df.reset_index(drop=True)

    all_contacts = pd.concat([deduped_main_contact_school_df, deduped_main_contact_district_df], axis=0,ignore_index=False)
    all_contacts['Email Address'] = all_contacts['Email Address'].str.lower()
    all_contacts = school.dedupeContactData(all_contacts)
    # all_contacts.to_csv(f'all_contacts_df{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

    deduped_main_contact_district_df.reset_index(inplace=True)
    deduped_main_contact_school_df.reset_index(inplace=True)
    all_contacts.reset_index(inplace=True)

    session['schooldata'] = deduped_main_school_df.to_json()
    # session['districtcontactdata'] = deduped_main_contact_district_df.to_json()
    # session['schoolcontactdata'] = deduped_main_contact_school_df.to_json()
    session['state'] = state
    session['allcontacts'] = all_contacts.to_json()
    



    

#|----------------------------------------------------------------------------------------------------|
#|---------------------------------------  Salesforce CONTACT ----------------------------------------|
#|----------------------------------------------------------------------------------------------------|
    
#     salesforce_session = SalesforceConnection(username, password, security_token)
#     # mark_to_delete_email_opt_out_response = session.connect().query('SELECT Id, Name, Email FROM Contact WHERE (Mark_for_Delete__c = true OR HasOptedOutOfEmail = true) AND MailingState = ' + "'" + state + "'")
#     # mark_to_delete_email_opt_out_salesforce_contacts = pd.DataFrame(mark_to_delete_email_opt_out_response['records']).drop(labels='attributes', axis=1)


    # # Query and Rename Email column to Email Address in order to dedupe
    # mark_to_delete_email_opt_out_salesforce_contacts = district.queryForMarkForDeleteAndEmailOptOutContactsSFData(state)

    # contacts_to_import_df = pd.DataFrame(all_contacts)
    # # contacts_to_import_df.to_csv(f'all_contacts_SF_SECTION{today}.csv', index=False, float_format='%.0f')

    # if not mark_to_delete_email_opt_out_salesforce_contacts.empty:

    # #Concatenate SF Data with K12 Data
    #     contacts_to_import_df = pd.concat([contacts_to_import_df, mark_to_delete_email_opt_out_salesforce_contacts], axis=0,ignore_index=False)    

    #     duplicate_contacts_from_mark_to_delete_email_opt_out = district.returnDuplicateContactsBetweenK12andSalesforce(contacts_to_import_df)
    #     duplicate_contacts_from_mark_to_delete_email_opt_out.to_csv(f'Contacts Marked To Delete Or Email Opt Out{today}.csv', index=False, float_format='%.0f')


    #     # mark_to_delete_email_opt_out_deduped_contacts.drop_duplicates(subset="Email Address", inplace=True, keep=False)
    #     contacts_to_import_df = district.removeAllDuplicatesBetweenK12andSalesforce(contacts_to_import_df)

    #     contacts_to_import_df.to_csv(f'all_contacts_deduped_with_salesforce_df{today}.csv', index=False, float_format='%.0f')

    # salesforce_all_contacts_by_state = district.queryAllContactsByState(state)
    # salesforce_all_contacts_by_state.to_csv(f'salesforce_all_contacts_by_state{today}.csv', index=False, float_format='%.0f')

    # if not salesforce_all_contacts_by_state.empty:
    # #     current_customer_and_active_prospect_contacts = district.returnContactsFromQuery(current_customer_and_active_prospect_contacts)
    # #     current_customer_and_active_prospect_contacts.to_csv(f'current_customer_and_active_prospect_contacts{today}.csv', index=False, float_format='%.0f')


    #     k12data_check_with_df_data = contacts_to_import_df.merge(salesforce_all_contacts_by_state, on =['Email Address'],how='outer', indicator=True)

    #     contacts_to_import_df = k12data_check_with_df_data[k12data_check_with_df_data['_merge'] == 'left_only']
    #     contacts_for_contact_history = k12data_check_with_df_data[k12data_check_with_df_data['_merge'] == 'both']
    #     #'left_only' existis in K-12 Data and not in SF
    #     #'right_only' exists in Salesforce and not in K-12
    #     #'both' exists in both data sets

    #     contacts_for_contact_history = contacts_for_contact_history[(contacts_for_contact_history['Nces School Id'].isnull() & contacts_for_contact_history['District Id'].ne(contacts_for_contact_history['DistrictId'])) | (contacts_for_contact_history['District Id'].isnull() & contacts_for_contact_history['Nces School Id'].ne(contacts_for_contact_history['SchoolId']))]
    #     contacts_for_contact_history.drop(labels='_merge', axis=1, inplace=True)


    #     if not contacts_for_contact_history.empty:
    #         contacts_for_contact_history.rename(columns={'Id': 'ContactId'}, inplace=True)
    #         contacts_for_contact_history.to_csv(f'contacts_for_contact_history_initial_if{today}.csv', index=False, float_format='%.0f')

    #         contact_history_report = district.queryContactHistory()
    #         contact_history_contacts = district.returnContactsFromContactHistoryQuery(contact_history_report)
    #         contact_history_report = contact_history_report.merge(contact_history_contacts, on =['ContactId'],how='right')
    #         contact_history_report.drop(labels='Contact', axis=1,inplace=True)

    #         contact_history_report.sort_values('CreatedDate', ascending=False,inplace=True)
    #         contact_history_report.drop_duplicates(subset="ContactId",keep='first', inplace=True)
    #         contact_history_report.rename(columns={'Email Address': 'Salesforce Email Address'}, inplace=True)
    #         contact_history_report.to_csv(f'Contact History Report{today}.csv', index=False, float_format='%.0f')

    #         contacts_in_history_report = contacts_for_contact_history.merge(contact_history_report, on =['ContactId'],how='outer', indicator=True)
    #         # contacts_in_history_report.sort_values('CreatedDate', ascending=False,inplace=True)
    #         # contacts_in_history_report.drop_duplicates(subset="ContactId",keep='first', inplace=True)
            


    #         contacts_in_history_report_with_different_nces_id = contacts_in_history_report[contacts_in_history_report['_merge'] == 'left_only']
    #         contacts_in_history_report_with_different_nces_id.rename(columns={'Email Address_x': 'Email Address', 'Sort_x': 'Sort', 'Source_x':'Source'}, inplace=True)
    #         # contacts_in_history_report_with_different_nces_id = district.buildExistingContactSchema(contacts_in_history_report_with_different_nces_id)
    #         contacts_in_history_report_with_different_nces_id.drop(labels={'Sort','Source'}, axis=1,inplace=True)

    #         contacts_in_history_report_with_different_nces_id.to_csv(f'{state} - Contacts with Different NCES ID and Not In Contact History Report{today}.csv', index=False, float_format='%.0f')

    #         contacts_in_history_report = contacts_in_history_report[contacts_in_history_report['_merge'] == 'both']

    #         contacts_in_history_report = contacts_in_history_report[((contacts_in_history_report['NewValue'] == '000000000') & (contacts_in_history_report['District Id'].ne(contacts_in_history_report['OldValue']))) | (contacts_in_history_report['District Id'].ne(contacts_in_history_report['NewValue'])) | ((contacts_in_history_report['NewValue'] == '000000000') & (contacts_in_history_report['Nces School Id'].ne(contacts_in_history_report['OldValue']))) | (contacts_in_history_report['Nces School Id'].ne(contacts_in_history_report['NewValue']))]
            

    #         # contacts_to_import_df = pd.concat([contacts_to_import_df, contacts_in_history_report], axis=0,ignore_index=False)
    #         contacts_to_import_df.rename(columns={'Sort_x': 'Sort', 'Source_x': 'Source'}, inplace=True)
    #         contacts_to_import_df = district.buildSchoolContactSchema(contacts_to_import_df)

    #         contacts_in_history_report.to_csv(f'Contacts to Update From Contact History Report{today}.csv', index=False, float_format='%.0f')
    #         contacts_to_import_df.to_csv(f'{state} - Contacts Not In Salesforce{today}.csv', index=False, float_format='%.0f')

    #         district_accounts_by_state = district.queryDistrictAccountsByState(state)
    #         # district_accounts_by_state.to_csv(f'district_accounts_by_state{today}.csv', index=False, float_format='%.0f')

    #         school_accounts_by_state = district.querySchoolAccountsByState(state)
    #         # school_accounts_by_state.to_csv(f'school_accounts_by_state{today}.csv', index=False, float_format='%.0f')

    #         school_contact_account_merge = contacts_to_import_df.merge(school_accounts_by_state, on =['Nces School Id'],how='outer', indicator=True)
    #         # school_contact_account_merge.to_csv(f'school_contact_account_merge{today}.csv', index=False, float_format='%.0f')

    #         school_contacts_with_existing_salesforce_account = school_contact_account_merge[school_contact_account_merge['_merge'] == 'both']
    #         school_contacts_with_existing_salesforce_account.rename(columns={'Id':'AccountId'},inplace=True)
    #         school_contacts_with_existing_salesforce_account.drop(labels='_merge', axis=1,inplace=True)

    #         # school_contacts_with_existing_salesforce_account.to_csv(f'school_contacts_with_existing_salesforce_account{today}.csv', index=False, float_format='%.0f')

    #         school_contacts_without_salesforce_account = school_contact_account_merge[school_contact_account_merge['_merge'] == 'left_only']
    #         school_contacts_without_salesforce_account.drop(labels={'_merge','Id','Account Name','Type'}, axis=1,inplace=True)



    #         district_contact_account_merge = school_contacts_without_salesforce_account.merge(district_accounts_by_state, on =['District Id'],how='outer', indicator=True)
    #         # district_contact_account_merge.to_csv(f'district_contact_account_merge{today}.csv', index=False, float_format='%.0f')

    #         district_contacts_with_existing_salesforce_account = district_contact_account_merge[district_contact_account_merge['_merge'] == 'both']
    #         district_contacts_with_existing_salesforce_account.rename(columns={'Id':'AccountId'},inplace=True)
    #         district_contacts_with_existing_salesforce_account.drop(labels='_merge', axis=1,inplace=True)

    #         # district_contacts_with_existing_salesforce_account.to_csv(f'district_contacts_with_existing_salesforce_account{today}.csv', index=False, float_format='%.0f')

            
            

    #         contacts_with_existing_salesforce_account = pd.concat([school_contacts_with_existing_salesforce_account, district_contacts_with_existing_salesforce_account], axis=0,ignore_index=False)    
    #         contacts_with_existing_salesforce_account.to_csv(f'{state} - Contacts with Existing Salesforce Accounts{today}.csv', index=False, float_format='%.0f')



    #         contacts_without_existing_salesforce_account = district_contact_account_merge[district_contact_account_merge['_merge'] == 'left_only']
    #         contacts_without_existing_salesforce_account.to_csv(f'{state} - Contacts with No Salesforce Account{today}.csv', index=False, float_format='%.0f')

    #         session['contactsWithNoSalesforceAccount'] = contacts_without_existing_salesforce_account.to_json()


    if not district_accounts_by_state.empty:

        resp = make_response(district_accounts_not_in_salesforce.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=District Accounts to Import.csv"
        resp.headers["Content-Type"] = "text/csv"
        
        return resp
    else:
        resp = make_response(deduped_main_district_df.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=District Accounts to Import.csv"
        resp.headers["Content-Type"] = "text/csv"
        deduped_main_district_df.to_csv(f'{state} - District Accounts To Import{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        
        return resp

@app.route("/importdata")
def importData():
    return render_template("importdata.html")


@app.route("/createpublicschools", methods=["GET","POST"])
def createpublicschools():

    schooldata = session.get('schooldata')
    schooldata = pd.read_json(schooldata, dtype=False)
    schooldata["Data Source Import Date"] = pd.to_datetime(schooldata["Data Source Import Date"],dayfirst=True)
    # schooldata.to_csv(f'SCHOOL DATA{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

    state = session.get('state', None)

    district = DataHelper()



    # districtcontactdata = session.get('districtcontactdata')
    # districtcontactdata = pd.read_json(districtcontactdata, dtype=False)

    

    # response = request.form['file']

    # for file in os.listdir(response):
    #     if 'success' in file and file.endswith('.csv'):
    #         dc = pd.read_csv(os.path.join(response,file),dtype={'NCES SCHOOL ID': str, 'DISTRICT ID': str})
    #         dc = dc[['ID','DISTRICT ID']].copy()
    #         dc.rename(columns={'DISTRICT ID':'District Id', 'ID':'Id'},inplace=True)
    #         district_accounts_by_state = district.queryDistrictAccountsByState(state)
    #         dc = pd.concat([dc,district_accounts_by_state],ignore_index=False)
            # dc['District Id']=dc['District Id'].astype(int)
    


    school_accounts_by_state = district.querySchoolAccountsByState(state)
    if not school_accounts_by_state.empty:
        school_accounts_by_state.to_csv(f'Public Schools from Salesforce - {today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        school_accounts_by_state.drop(labels={'Account Name'}, axis=1, inplace=True)

        merged_school_accounts_with_salesforce_data = schooldata.merge(school_accounts_by_state, on =['Nces School Id'],how='outer', indicator=True)
        # merged_school_accounts_with_salesforce_data.to_csv(f'merged_school_accounts_with_salesforce_data.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

        school_accounts_not_in_salesforce = merged_school_accounts_with_salesforce_data[merged_school_accounts_with_salesforce_data['_merge'] == 'left_only']
        school_accounts_not_in_salesforce.drop(labels={'_merge','Id','Type','ParentId'}, axis=1, inplace=True)
        school_accounts_not_in_salesforce.reset_index(inplace=True)

        # school_accounts_not_in_salesforce.to_csv(f'school_accounts_not_in_salesforce.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

        school_accounts_to_upsert = merged_school_accounts_with_salesforce_data[merged_school_accounts_with_salesforce_data['_merge'] == 'both']
        school_accounts_to_upsert.drop(labels={'_merge','Type'}, axis=1, inplace=True)
        school_accounts_to_upsert.reset_index(inplace=True)
        # school_accounts_to_upsert.to_csv(f'school_accounts_to_upsert.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    

        dc = district.queryDistrictAccountsByState(state)
        dc.drop(labels={'Dont_Edit__c'}, axis=1, inplace=True)
        # dc.to_csv(f'DC   .csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        # dc.rename(columns={'Id':'ParentId'},inplace=True)
        if not dc.empty:
            dc.drop(labels={'Account Name', 'Type'}, axis=1, inplace=True)

            school_accounts_with_salesforce_account_merge = school_accounts_not_in_salesforce.merge(dc, on =['District Id'],how='outer', indicator=True)

            # school_accounts_with_salesforce_account_merge.to_csv(f'school_accounts_not_in_salesforce_AFTER_MERGE.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

            school_accounts_with_salesforce_account = school_accounts_with_salesforce_account_merge[school_accounts_with_salesforce_account_merge['_merge'] == 'left_only']

            # school_accounts_with_salesforce_account_merge.to_csv(f'school_accounts_not_in_salesforce_LEFT_ONLY.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

            if not school_accounts_with_salesforce_account.empty:
                school_accounts_with_salesforce_account = district.buildSchoolToDistrictSchema(school_accounts_with_salesforce_account)
                school_accounts_with_salesforce_account = district.dedupeDistrictData(school_accounts_with_salesforce_account)
                school_accounts_with_salesforce_account = district.populateSchoolAccountWithNoDistrictDefaultValues(school_accounts_with_salesforce_account)
                school_accounts_with_salesforce_account.to_csv(f'School Accounts Without a District Id Account.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
                return render_template("reuploaddistrict.html")

            school_accounts_with_salesforce_account = school_accounts_with_salesforce_account_merge[school_accounts_with_salesforce_account_merge['_merge'] == 'both']
            # school_accounts_with_salesforce_account.to_csv(f'school_accounts_with_salesforce_account.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
            school_accounts_with_salesforce_account.rename(columns={'Id':'ParentId'},inplace=True)
            # school_accounts_with_salesforce_account.to_csv(f'school_accounts_with_salesforce_account_parent.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        # vlookup District Contacts with District Account with Salesforce Id's


        # districtcontactdata = dc.merge(districtcontactdata, on =['District Id'],how='right')
             
            school_accounts_with_salesforce_account.drop(labels={'_merge', 'Temporary Name', 'index', 'level_0'}, axis=1, inplace=True)
            school_accounts_to_upsert_concatenated = pd.concat([school_accounts_to_upsert, school_accounts_with_salesforce_account], axis=0,ignore_index=False)   
            # school_accounts_to_upsert_concatenated.rename(columns={'Id':'Parent Account ID'},inplace=True)
            # school_accounts_with_salesforce_account.rename(columns={'Id':'Parent Account ID'},inplace=True)
            school_accounts_to_upsert_concatenated['Dont_Edit__c'] = np.where(school_accounts_to_upsert_concatenated['Dont_Edit__c'].isnull(),False,school_accounts_to_upsert_concatenated['Dont_Edit__c'])
            school_accounts_to_upsert_concatenated = school_accounts_to_upsert_concatenated[school_accounts_to_upsert_concatenated['Dont_Edit__c'] == False]
            school_accounts_with_salesforce_account.to_csv(f'Public Schools to Import{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
            school_accounts_to_upsert_concatenated.to_csv(f'Public Schools to Upsert{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
            session['schooldata'] = []
            return render_template("matchcontacts.html")
        # districtcontactdata.to_csv(f'district_contacts_df{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    else:
        dc = district.queryDistrictAccountsByState(state)
        if not dc.empty:
            dc.drop(labels={'Account Name', 'Type','Dont_Edit__c'}, axis=1, inplace=True)

            school_accounts_with_salesforce_account_merge = schooldata.merge(dc, on =['District Id'],how='outer', indicator=True)

            school_accounts_with_salesforce_account = school_accounts_with_salesforce_account_merge[school_accounts_with_salesforce_account_merge['_merge'] == 'left_only']

            if not school_accounts_with_salesforce_account.empty:
                school_accounts_with_salesforce_account = district.buildSchoolToDistrictSchema(school_accounts_with_salesforce_account)
                school_accounts_with_salesforce_account = district.dedupeDistrictData(school_accounts_with_salesforce_account)
                school_accounts_with_salesforce_account.to_csv(f'School Accounts Without a District Id Account.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
                return render_template("reuploaddistrict.html")

            school_accounts_with_salesforce_account = school_accounts_with_salesforce_account_merge[school_accounts_with_salesforce_account_merge['_merge'] == 'both']
            school_accounts_with_salesforce_account.rename(columns={'Id':'Parent Account ID'},inplace=True)
        # vlookup District Contacts with District Account with Salesforce Id's


        # districtcontactdata = dc.merge(districtcontactdata, on =['District Id'],how='right')
            school_accounts_with_salesforce_account.drop(labels={'_merge', 'Temporary Name', 'index'}, axis=1, inplace=True)
            school_accounts_with_salesforce_account.to_csv(f'Public Schools to Import{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
            # session['schooldata'] = []
            return render_template("matchcontacts.html")
        else:
            return render_template("nodistrictaccounts.html")

@app.route("/matchcontacts", methods=["GET","POST"])
def matchcontacts():

    district = DataHelper()

    state = session.get('state', None)

    all_contacts = session.get('allcontacts')
    contacts_to_import_df = pd.read_json(all_contacts, dtype=False)
    contacts_to_import_df["Data Source Import Date"] = pd.to_datetime(contacts_to_import_df["Data Source Import Date"],dayfirst=True)
    # contacts_to_import_df.to_csv(f'contacts_to_import_df.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    
    


    # Query and Rename Email column to Email Address in order to dedupe
    mark_to_delete_email_opt_out_salesforce_contacts = district.queryForMarkForDeleteAndEmailOptOutContactsSFData(state)

    if not mark_to_delete_email_opt_out_salesforce_contacts.empty:

    #Concatenate SF Data with K12 Data
        contacts_to_import_df = pd.concat([contacts_to_import_df, mark_to_delete_email_opt_out_salesforce_contacts], axis=0,ignore_index=False)    

        duplicate_contacts_from_mark_to_delete_email_opt_out = district.returnDuplicateContactsBetweenK12andSalesforce(contacts_to_import_df)
        duplicate_contacts_from_mark_to_delete_email_opt_out.to_csv(f'Contacts Marked To Delete Or Email Opt Out{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')


        # mark_to_delete_email_opt_out_deduped_contacts.drop_duplicates(subset="Email Address", inplace=True, keep=False)
        contacts_to_import_df = district.removeAllDuplicatesBetweenK12andSalesforce(contacts_to_import_df)
        # contacts_for_contact_history.to_csv(f'contacts_FOR_contact_history.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    
    salesforce_all_contacts_by_state = district.queryAllContactsByState(state)
    # salesforce_all_contacts_by_state.to_csv(f'salesforce_all_contacts_by_state{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

    if not salesforce_all_contacts_by_state.empty:

        k12data_check_with_df_data = contacts_to_import_df.merge(salesforce_all_contacts_by_state, on =['Email Address'],how='outer', indicator=True)
        # k12data_check_with_df_data.to_csv(f'k12data_check_with_df_data.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')


        #!!
        contacts_to_import_df = k12data_check_with_df_data[k12data_check_with_df_data['_merge'] == 'left_only']
        contacts_to_import_df.drop(labels='_merge', axis=1, inplace=True)
        contacts_for_contact_history = k12data_check_with_df_data[k12data_check_with_df_data['_merge'] == 'both']
        # contacts_for_contact_history.to_csv(f'contacts_FOR_contact_history.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        
        # contacts_for_contact_history = contacts_for_contact_history[(contacts_for_contact_history['Nces School Id'].isnull() & contacts_for_contact_history['District Id'].ne(contacts_for_contact_history['DistrictId']) & ~contacts_for_contact_history['DistrictId'].isnull()) | (contacts_for_contact_history['District Id'].isnull() & contacts_for_contact_history['Nces School Id'].ne(contacts_for_contact_history['SchoolId']) & ~contacts_for_contact_history['SchoolId'].isnull())]
        contacts_for_contact_history = contacts_for_contact_history[((contacts_for_contact_history['Source_x'].eq('Public File') | contacts_for_contact_history['Source_x'].eq('Personnel File')) & contacts_for_contact_history['Nces School Id'].ne(contacts_for_contact_history['SchoolId']) & contacts_for_contact_history['Dont_Edit__c'].eq(False)) | ((contacts_for_contact_history['Source_x'].eq('District File') | contacts_for_contact_history['Source_x'].eq('Superintendent File')) & contacts_for_contact_history['District Id'].ne(contacts_for_contact_history['DistrictId']) & contacts_for_contact_history['Dont_Edit__c'].eq(False))]
        # contacts_for_contact_history.to_csv(f'contacts_FOR_contact_historyV2-1.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        contacts_for_contact_history.drop(labels='_merge', axis=1, inplace=True)

        

        if not contacts_for_contact_history.empty:

            if 'Id_x' and 'Id_y' in contacts_for_contact_history:
                contacts_for_contact_history['ContactId'] = np.where(~contacts_for_contact_history['Id_y'].isnull(),contacts_for_contact_history['Id_y'],np.where(~contacts_for_contact_history['Id_x'].isnull(),contacts_for_contact_history['Id_x'],contacts_for_contact_history['Id_y']))
                # contacts_for_contact_history['Name'] = np.where(~contacts_for_contact_history['Name_y'].isnull(),contacts_for_contact_history['Name_y'],np.where(~contacts_for_contact_history['Name_x'].isnull(),contacts_for_contact_history['Name_x'],contacts_for_contact_history['Name_y']))
                # contacts_for_contact_history.rename(columns={'Id': 'ContactId'}, inplace=True)
            # contacts_for_contact_history.to_csv(f'contacts_for_contact_history.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
            contact_history_report = district.queryContactHistory()
            if not contact_history_report.empty:
                # contact_history_report.to_csv(f'contact_history_report.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
                contact_history_contacts = district.returnContactsFromContactHistoryQuery(contact_history_report)
                contact_history_report = contact_history_report.merge(contact_history_contacts, on =['ContactId'],how='right')
                contact_history_report.drop(labels='Contact', axis=1,inplace=True)

                contact_history_report.sort_values('CreatedDate', ascending=False,inplace=True)
                contact_history_report.drop_duplicates(subset="ContactId",keep='first', inplace=True)
                contact_history_report.rename(columns={'Email Address': 'Salesforce Email Address'}, inplace=True)
                contact_history_report.to_csv(f'Contact History Report{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

                contacts_in_history_report = contacts_for_contact_history.merge(contact_history_report, on =['ContactId'],how='outer', indicator=True)

                contacts_in_history_report_with_different_nces_id = contacts_in_history_report[contacts_in_history_report['_merge'] == 'left_only']

                if not contacts_in_history_report_with_different_nces_id.empty:
                    contacts_in_history_report_with_different_nces_id.rename(columns={'Email Address_x': 'Email Address', 'Sort_x': 'Sort', 'Source_x':'Source'}, inplace=True)
                    contacts_in_history_report_with_different_nces_id.drop(labels={'Sort','Source'}, axis=1,inplace=True)
                    contacts_in_history_report_with_different_nces_id.to_csv(f'{state} - Contacts with Different NCES ID and Not In Contact History Report{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

                contacts_in_history_report = contacts_in_history_report[contacts_in_history_report['_merge'] == 'both']
                contacts_in_history_report.drop(labels='_merge', axis=1, inplace=True)

                # contacts_in_history_report.to_csv(f'contacts_in_history_reportBEFORE CHECK.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

                contacts_in_history_report = contacts_in_history_report[((contacts_in_history_report['NewValue'].eq('00000000')) & (contacts_in_history_report['District Id'].ne(contacts_in_history_report['OldValue']) & contacts_in_history_report['Nces School Id'].ne(contacts_in_history_report['OldValue']) )) | ((contacts_in_history_report['NewValue'].ne('00000000')) & (contacts_in_history_report['District Id'].ne(contacts_in_history_report['NewValue']) & contacts_in_history_report['Nces School Id'].ne(contacts_in_history_report['NewValue']) ))]
                #  | (contacts_in_history_report['District Id'].ne(contacts_in_history_report['NewValue'])) | (contacts_in_history_report['Nces School Id'].ne(contacts_in_history_report['NewValue']))]


                # contacts_in_history_report = contacts_in_history_report[(contacts_in_history_report['NewValue'] == '000000000' & (contacts_in_history_report['District Id'].ne(contacts_in_history_report['OldValue']) | contacts_in_history_report['Nces School Id'].ne(contacts_in_history_report['OldValue'])))]

                # contacts_in_history_report.to_csv(f'contacts_in_history_reportAFTER CHECK.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

                district_accounts = district.queryDistrictAccountsByState(state)
                school_accounts = district.querySchoolAccountsByState(state)


                match_school_contacts_with_salesforce_accounts = contacts_in_history_report.merge(school_accounts, on =['Nces School Id'],how='outer', indicator=True)
                success_match_school_contacts_with_salesforce_accounts = match_school_contacts_with_salesforce_accounts[match_school_contacts_with_salesforce_accounts['_merge'] == 'both']
                success_match_school_contacts_with_salesforce_accounts.drop(labels='_merge', axis=1, inplace=True)
                # success_match_school_contacts_with_salesforce_accounts.to_csv(f'success_match_school_contacts_with_salesforce_accounts.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')


                district_contacts_with_salesforce_accounts = match_school_contacts_with_salesforce_accounts[match_school_contacts_with_salesforce_accounts['_merge'] == 'left_only']
                district_contacts_with_salesforce_accounts.drop(labels={'Id','Account Name_y','Type_y','_merge'}, axis=1,inplace=True)

                match_district_contacts_with_salesforce_accounts = district_contacts_with_salesforce_accounts.merge(district_accounts, on =['District Id'],how='outer', indicator=True)
                success_match_district_contacts_with_salesforce_accounts = match_district_contacts_with_salesforce_accounts[match_district_contacts_with_salesforce_accounts['_merge'] == 'both']
                success_match_district_contacts_with_salesforce_accounts.drop(labels='_merge', axis=1, inplace=True)
                # success_match_district_contacts_with_salesforce_accounts.to_csv(f'success_match_district_contacts_with_salesforce_accounts.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')


                contacts_to_update_from_history_report = pd.concat([success_match_district_contacts_with_salesforce_accounts, success_match_school_contacts_with_salesforce_accounts], axis=0,ignore_index=False)    
                
                contacts_to_update_from_history_report['Account Type'] = np.where(contacts_to_update_from_history_report['Type'].notnull(),contacts_to_update_from_history_report['Type'],contacts_to_update_from_history_report['Type_y'])
                contacts_to_update_from_history_report['Account Name'] = np.where(contacts_to_update_from_history_report['Account Name'].notnull(),contacts_to_update_from_history_report['Account Name'],contacts_to_update_from_history_report['Account Name_y'])
                contacts_to_update_from_history_report['Name'] = np.where(contacts_to_update_from_history_report['Name_x'].notnull(),contacts_to_update_from_history_report['Name_x'],contacts_to_update_from_history_report['Name_y'])

                

                contacts_to_update_from_history_report = district.buildContactUpdate(contacts_to_update_from_history_report)

                

                # account_types = ['Unqualified','Researching','Call Attempt 1','Call Attempt 2','Call Attempt 3','Call Attempt 4','Call Attempt 5','Call Attempt 6','Pool - Not Interested','Pool - Lack of Funding','Former Customer','Pool (Not Interested)','Pool (Lack of Funding)']
                # contacts_to_update_from_history_report = contacts_to_update_from_history_report[~contacts_to_update_from_history_report['Account Type'].isin(account_types)]
                
                # contacts_with_old_account_id = district.queryContactHistoryAccountId()

                # contacts_in_history_report = contacts_in_history_report.merge(contacts_with_old_account_id, on =['ContactId'],how='outer', indicator=True)
                # contacts_in_history_report = contacts_in_history_report[contacts_in_history_report['_merge'] == 'both']

                # contacts_to_import_df = pd.concat([contacts_to_import_df, contacts_in_history_report], axis=0,ignore_index=False)
                contacts_to_import_df.rename(columns={'Sort_x': 'Sort', 'Source_x': 'Source'}, inplace=True)
                contacts_to_import_df = district.buildSchoolContactSchema(contacts_to_import_df)

                # contacts_in_history_report = district.buildContactUpdate(contacts_in_history_report)
                

                # if not contacts_in_history_report_with_different_nces_id.empty:
                if not contacts_to_update_from_history_report.empty:
                    contacts_to_update_from_history_report.to_csv(f'Contacts to Update From Contact History Report{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
            

    district_accounts_by_state = district.queryDistrictAccountsByState(state)
    school_accounts_by_state = district.querySchoolAccountsByState(state)

    # contacts_to_import_df.drop(labels={'_merge','Id','Type','Account Name','AccountId','DistrictId','SchoolId','Name','Source_y','Sort_y'}, axis=1, inplace=True) 
    # contacts_to_import_df.to_csv(f'contacts_to_import_df{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

    school_contact_account_merge = contacts_to_import_df.merge(school_accounts_by_state, on =['Nces School Id'],how='outer', indicator=True)
    # school_contact_account_merge.to_csv(f'school_contact_account_merge{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

    school_contacts_with_existing_salesforce_account = school_contact_account_merge[school_contact_account_merge['_merge'] == 'both']
    # school_contacts_with_existing_salesforce_account.to_csv(f'school_contacts_with_existing_salesforce_account{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    # school_contacts_with_existing_salesforce_account.rename(columns={'Id':'AccountId'},inplace=True)
    school_contacts_with_existing_salesforce_account.drop(labels='_merge', axis=1,inplace=True)
    # school_contacts_with_existing_salesforce_account.reset_index(inplace=True, drop=True)

    

    school_contacts_without_salesforce_account = school_contact_account_merge[school_contact_account_merge['_merge'] == 'left_only']
    # school_contacts_with_existing_salesforce_account.to_csv(f'school_contacts_with_existing_salesforce_account{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    # school_contacts_without_salesforce_account.to_csv(f'school_contacts_without_salesforce_account{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    if not school_contacts_without_salesforce_account.empty:
        # school_contacts_without_salesforce_account.to_csv(f'school_contacts_without_salesforce_account{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        # school_contacts_without_salesforce_account.drop(labels={'_merge','Id_y','Account Name_y','Type_y', 'Source_y', 'Sort_y','Name'}, axis=1,inplace=True)
        school_contacts_without_salesforce_account.drop(labels={'_merge'}, axis=1,inplace=True)



        district_contact_account_merge = school_contacts_without_salesforce_account.merge(district_accounts_by_state, on =['District Id'],how='outer', indicator=True)
    # district_contact_account_merge.to_csv(f'district_contact_account_merge{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

        district_contacts_with_existing_salesforce_account = district_contact_account_merge[district_contact_account_merge['_merge'] == 'both']
        # district_contacts_with_existing_salesforce_account.rename(columns={'Id':'AccountId'},inplace=True)
        district_contacts_with_existing_salesforce_account.drop(labels='_merge', axis=1,inplace=True)
        # district_contacts_with_existing_salesforce_account.reset_index(inplace=True, drop=True)
        

    
    

        contacts_with_existing_salesforce_account = pd.concat([school_contacts_with_existing_salesforce_account, district_contacts_with_existing_salesforce_account], axis=0,ignore_index=False)    
        contacts_with_existing_salesforce_account = district.populateContactDefaultValues(contacts_with_existing_salesforce_account)
        contacts_with_existing_salesforce_account['AccountId'] = np.where(~contacts_with_existing_salesforce_account['Id_y'].isnull(),contacts_with_existing_salesforce_account['Id_y'],np.where(~contacts_with_existing_salesforce_account['Id_x'].isnull(),contacts_with_existing_salesforce_account['Id_x'],contacts_with_existing_salesforce_account['Id']))
        contacts_with_existing_salesforce_account.drop(labels={'Id_y','Id_x','Id'}, axis=1,inplace=True)
        contacts_with_existing_salesforce_account['Account'] = np.where(~contacts_with_existing_salesforce_account['Account Name_y'].isnull(),contacts_with_existing_salesforce_account['Account Name_y'],np.where(~contacts_with_existing_salesforce_account['Account Name_x'].isnull(),contacts_with_existing_salesforce_account['Account Name_x'],contacts_with_existing_salesforce_account['Account Name']))
        contacts_with_existing_salesforce_account.drop(labels={'Account Name_y','Account Name_x','Account Name'}, axis=1,inplace=True)
        contacts_with_existing_salesforce_account['Account Type'] = np.where(~contacts_with_existing_salesforce_account['Type_y'].isnull(),contacts_with_existing_salesforce_account['Type_y'],np.where(~contacts_with_existing_salesforce_account['Type_x'].isnull(),contacts_with_existing_salesforce_account['Type_x'],contacts_with_existing_salesforce_account['Type']))
        contacts_with_existing_salesforce_account.drop(labels={'Type_y','Type_x','Type'}, axis=1,inplace=True)

        contacts_with_existing_salesforce_account.rename(columns={'Account': 'Account Name', 'Account Type': 'Type'}, inplace=True)
        # contacts_with_existing_salesforce_account.to_csv(f'contacts_with_existing_salesforce_account{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        # contacts_with_existing_salesforce_account.drop(labels={'DistrictId','SchoolId','Name', 'Source_y', 'Sort_y'}, axis=1,inplace=True)
        contacts_with_existing_salesforce_account.to_csv(f'{state} - Contacts To Upload{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

        contacts_without_existing_salesforce_account = district_contact_account_merge[district_contact_account_merge['_merge'] == 'left_only']
        if not contacts_without_existing_salesforce_account.empty:
            contacts_without_existing_salesforce_account.to_csv(f'{state} - Contacts with No Salesforce Account{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    else:
        if 'Id_y' in school_contacts_with_existing_salesforce_account.columns:
            # school_contacts_with_existing_salesforce_account.to_csv(f'school_contacts_with_existing_salesforce_account{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
            school_contacts_with_existing_salesforce_account = district.populateContactDefaultValues(school_contacts_with_existing_salesforce_account)
            # school_contacts_with_existing_salesforce_account['AccountId'] = school_contacts_with_existing_salesforce_account['Id']

            school_contacts_with_existing_salesforce_account['AccountId'] = np.where(~school_contacts_with_existing_salesforce_account['Id_y'].isnull(),school_contacts_with_existing_salesforce_account['Id_y'],school_contacts_with_existing_salesforce_account['Id_x'])
            school_contacts_with_existing_salesforce_account.drop(labels={'Id_y','Id_x'}, axis=1,inplace=True)
            
            school_contacts_with_existing_salesforce_account['Account Name'] = np.where(~school_contacts_with_existing_salesforce_account['Account Name_y'].isnull(),school_contacts_with_existing_salesforce_account['Account Name_y'],school_contacts_with_existing_salesforce_account['Account Name_x'])
            school_contacts_with_existing_salesforce_account.drop(labels={'Account Name_y','Account Name_x'}, axis=1,inplace=True)

            school_contacts_with_existing_salesforce_account['Account Type'] = np.where(~school_contacts_with_existing_salesforce_account['Type_y'].isnull(),school_contacts_with_existing_salesforce_account['Type_y'],school_contacts_with_existing_salesforce_account['Type_x'])
            school_contacts_with_existing_salesforce_account.drop(labels={'Type_y','Type_x'}, axis=1,inplace=True)

            # school_contacts_with_existing_salesforce_account.drop(labels={'Id_y','Id_x'}, axis=1,inplace=True)
            # school_contacts_with_existing_salesforce_account['Account Name'] = np.where(~school_contacts_with_existing_salesforce_account['Account Name_y'].isnull(),school_contacts_with_existing_salesforce_account['Account Name_y'],school_contacts_with_existing_salesforce_account['Account Name_x'])
            # school_contacts_with_existing_salesforce_account.drop(labels={'Account Name_y','Account Name_x'}, axis=1,inplace=True)
            # school_contacts_with_existing_salesforce_account['Type'] = np.where(~school_contacts_with_existing_salesforce_account['Type_y'].isnull(),school_contacts_with_existing_salesforce_account['Type_y'],school_contacts_with_existing_salesforce_account['Type_x'])
            # school_contacts_with_existing_salesforce_account.drop(labels={'Type_y','Type_x'}, axis=1,inplace=True)
            # school_contacts_with_existing_salesforce_account.drop(labels={'DistrictId','SchoolId','Name', 'Source_y', 'Sort_y'}, axis=1,inplace=True)
            school_contacts_with_existing_salesforce_account.to_csv(f'{state} - Contacts To Upload{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
        else:
            school_contacts_with_existing_salesforce_account = district.populateContactDefaultValues(school_contacts_with_existing_salesforce_account)
            school_contacts_with_existing_salesforce_account.to_csv(f'{state} - Contacts To Upload{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')




    

    

    # schoolcontactdata = session.get('schoolcontactdata')
    # schoolcontactdata = pd.read_json(schoolcontactdata, dtype=False)

    # districtcontactdata = session.get('districtcontactdata')
    # districtcontactdata = pd.read_json(districtcontactdata, dtype=False)
    # districtcontactdata.to_csv(f'district_contacts2_df{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    

    # response = request.form['file']

    # for file in os.listdir(response):
    #     if 'success' in file and file.endswith('.csv'):
    #         dc = pd.read_csv(os.path.join(response,file),dtype='unicode')
    #         dc = dc[['ID','NCES SCHOOL ID']].copy()
    #         dc.rename(columns={'NCES SCHOOL ID':'Nces School Id'},inplace=True)

    # schoolcontactdata = dc.merge(schoolcontactdata, on = ['Nces School Id'], how='right')
    # schoolcontactdata.to_csv(f'school_contacts_df{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

    # all_contacts = pd.concat([districtcontactdata,schoolcontactdata],ignore_index=False)
    # all_contacts.rename(columns={'ID':'AccountId'},inplace=True)

    # all_contacts = district.populateContactDefaultValues(all_contacts)
    # districtcontactdata.to_csv(f'district_contacts3_df{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')
    # schoolcontactdata.to_csv(f'school_contacts2_df{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')

    # all_contacts = district.dedupeContactData(all_contacts)

    # all_contacts.to_csv(f'allcontact_df{today}.csv', index=False, float_format='%.0f', date_format='%d/%m/%Y')


    return render_template("checkmark.html")

if __name__=="__main__":
    app.run(host='127.0.0.1', port=8082,debug=True)

