from flask import Flask, render_template, request, send_file, redirect, url_for,make_response,jsonify
import pandas as pd
import numpy as np
import requests
from simple_salesforce import Salesforce
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

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/success-table", methods=['POST'])
def success_table():
    # response = request.form['file']
    session = SalesforceConnection(username, password, security_token)
    response = session.connect().query("SELECT Id, Name, Type,(SELECT Id, Name, Email FROM Contacts) FROM Account WHERE Id IN ('001VA000003jhL3YAI', '001VA000003jhL2YAI', '001VA000003jhL8YAI', '001VA000003jhLMYAY')")
    # print(jsonify(response['records']))
    df = pd.DataFrame(response['records']).drop(labels='attributes', axis=1)
    # contact_df = df['Contacts']
    # print(contact_df[0]['records'])
    # contact_df = contact_df.apply(lambda x:x['records'])
    contact2 = [dict(id=rec['Id'], Name=rec['Contacts']['records']) for rec in response['records']]
    contact2 = pd.DataFrame(contact2)


    # contact3 = contact2['Name'][1]
    # contact3 = pd.DataFrame(contact3).drop(labels='attributes', axis=1)

    contactList_df = pd.DataFrame()
    

    for i in range(0,len(contact2)): 
        contact3 = pd.DataFrame(contact2['Name'][i]).drop(labels='attributes', axis=1)
        contactList_df = pd.concat([contactList_df,contact3],ignore_index=True)

    # contactList_df = pd.DataFrame(contactList)
    # contactList_df = pd.concat([contactList_df],ignore_index=False)
    # contactList_df.drop(labels='attributes', axis=1, inplace=True)
    # print('Contact:')
    # print(contactList)

    # x = 0
    # while x < len(contact2):
    #     print(contact2['Name'][x])
    #     x = x + 1

    

    print('Column Values:')
    print(contact2.columns.values)
    print('-----------------')
    print('CONTACT2')
    print(contact2['Name'])
    print('-----------------')
    print('CONTACT3')
    print(contact3['Name'])
    print('-----------------')
    # for cont in contact2:
    #     print(cont)

    
    # print(contact_df)
    df.to_json('records.json')
    # contact_df.to_json('contact.json')
    contact2.to_json('contact2.json')
    contact3.to_json('contact3.json')
    return render_template('success.html',  tables=[contactList_df.to_html(classes='data', index=True, justify='center', index_names=True)], titles=contactList_df.columns.values)

@app.route("/dedupe-k12", methods=["POST"])
def dedupek12():

    district_df = []
    school_df = []

    response = request.form['file']
    state = request.form['state']

    district = DataHelper()
    school = DataHelper()

    for file in os.listdir(response):
        dc = pd.read_excel(os.path.join(response,file))
        if 'superintendents' in file and file.endswith('.xlsx'):

            district.file = file
            district_df.append(district.readAndPrepareDistrictData(dc))

        elif 'district' in file and file.endswith('.xlsx'):

            district.file = file
            district_df.append(district.readAndPrepareDistrictData(dc))

        elif 'public' in file and file.endswith('.xlsx'):

            school.file = file
            school_df.append(school.readAndPeparePublicSchoolData(dc))

        elif 'school' in file and file.endswith('.xlsx'):
            
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

    #Create Schema for District and School Accounts
    main_district_df = district.buildDistrictSchema(district_df)
    main_school_df = school.buildSchoolSchema(school_df)

    #Concatenate Schools without School Name and Full School Name to District DataFrame
    if not schoolsToMergeAsDistricts.empty:
        main_district_df = pd.concat([main_district_df, schoolsToMergeAsDistricts], axis=0,ignore_index=False)


    

    #Dedupe District accounts by District Id and School accounts by NCES School Id
    deduped_main_district_df = district.dedupeDistrictData(main_district_df)

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
    
    deduped_main_school_df.to_csv(f'school_df{today}.csv', index=False, float_format='%.0f')
    deduped_main_district_df.to_csv(f'district_df{today}.csv', index=False, float_format='%.0f')

#|----------------------------------------------------------------------------------------------------|
#|---------------------------------------------  CONTACTS --------------------------------------------|
#|----------------------------------------------------------------------------------------------------|

    main_contact_district_df = district.buildDistrictContactSchema(district_df)
    main_contact_school_df = school.buildSchoolContactSchema(school_df)

    deduped_main_contact_district_df = district.dedupeContactData(main_contact_district_df)
    deduped_main_contact_school_df = school.dedupeContactData(main_contact_school_df)

    # deduped_main_contact_school_df.to_csv(f'contact_school_df{today}.csv', index=False, float_format='%.0f')
    # deduped_main_contact_district_df.to_csv(f'contact_district_df{today}.csv', index=False, float_format='%.0f')

    all_contacts = pd.concat([deduped_main_contact_school_df, deduped_main_contact_district_df], axis=0,ignore_index=False)
    all_contacts.to_csv(f'all_contacts_df{today}.csv', index=False, float_format='%.0f')
    

#|----------------------------------------------------------------------------------------------------|
#|---------------------------------------  Salesforce ------------------------------------------------|
#|----------------------------------------------------------------------------------------------------|
    
    # session = SalesforceConnection(username, password, security_token)
    # mark_to_delete_email_opt_out_response = session.connect().query('SELECT Id, Name, Email FROM Contact WHERE (Mark_for_Delete__c = true OR HasOptedOutOfEmail = true) AND MailingState = ' + "'" + state + "'")
    # mark_to_delete_email_opt_out_salesforce_contacts = pd.DataFrame(mark_to_delete_email_opt_out_response['records']).drop(labels='attributes', axis=1)


    # Query and Rename Email column to Email Address in order to dedupe
    mark_to_delete_email_opt_out_salesforce_contacts = district.checkForMarkForDeleteAndEmailOptOutContactsSFData(state)
    
    mark_to_delete_email_opt_out_salesforce_contacts.to_csv(f'salesforce_df{today}.csv', index=False, float_format='%.0f')

    #Concatenate SF Data with K12 Data
    mark_to_delete_email_opt_out_deduped_contacts = pd.concat([all_contacts, mark_to_delete_email_opt_out_salesforce_contacts], axis=0,ignore_index=False)    

    duplicate_contacts = district.returnDuplicateContactsBetweenK12andSalesforce(mark_to_delete_email_opt_out_deduped_contacts)
    duplicate_contacts.to_csv(f'duplicate_contacts{today}.csv', index=False, float_format='%.0f')


    # mark_to_delete_email_opt_out_deduped_contacts.drop_duplicates(subset="Email Address", inplace=True, keep=False)
    mark_to_delete_email_opt_out_deduped_contacts = district.removeAllDuplicatesBetweenK12andSalesforce(mark_to_delete_email_opt_out_deduped_contacts)

    mark_to_delete_email_opt_out_deduped_contacts.to_csv(f'all_contacts_deduped_with_salesforce_df{today}.csv', index=False, float_format='%.0f')


    resp = make_response(deduped_main_district_df.to_csv())
    resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
    resp.headers["Content-Type"] = "text/csv"
    
    return resp

if __name__=="__main__":
    app.run(host='127.0.0.1', port=8082,debug=True)
