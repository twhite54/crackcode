from asyncio.windows_events import NULL
import requests
import json
import time
import os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from requests.structures import CaseInsensitiveDict
import datetime
import math
import urllib.parse as urlparse
from msal import ConfidentialClientApplication
import msal
import pyodbc
import openai

########################################### Setup ##############################################
### Settings ###
# Clinical trials.gov
clinicaltrias_gov_print = False
print_ai_enhancements = True
run_ai = False
write_to_db = True
run_ai_enhancement = True
total_trials_to_process = 50  # Process X trials in total
total_pages_to_process = 1500

# ClinicalTrials.org
clinicaltrials_org_base_url = "https://clinicaltrials.gov/api/v2/studies"
clinicaltrials_org_statuses = ["RECRUITING", "NOT_YET_RECRUITING"]
clinicaltrials_org_page_size = 100  # Adjust as needed
clinicaltrials_org_page_token = None

# Open AI Key
enhancementclient = openai.OpenAI(api_key='sk-proj-daOk3CKpCHXfzoN99IDrT3BlbkFJgH4qdKIKmSN72ao24Zf4')

# Azure Key Vault
azure_key_vault_url = "https://dev-worldtrials-keyvault.vault.azure.net/"

# Text Analytics for Health
text_analytics_endpoint = 'https://dev-worldtirals-language.cognitiveservices.azure.com/'

# Variable to keep track of the current trial number
varCurrentTrialLoop = 0
varCurrentPage = 0

# Start fetching data
while varCurrentPage < total_pages_to_process:
    params = {
        "format": "json",
        "filter.overallStatus": "|".join(clinicaltrials_org_statuses),
        "pageSize": clinicaltrials_org_page_size,
        "pageToken": clinicaltrials_org_page_token,
    }

    response = requests.get(clinicaltrials_org_base_url, params=params)
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(response.text)
        break

    data = response.json()
    studies = data.get('studies', [])

    varJsonDumps = json.dumps(data)
    varJsonLoads = json.loads(varJsonDumps)
    #print(varJsonDumps)

    clinicaltrials_org_page_token = varJsonLoads.get("nextPageToken")
    #print(clinicaltrials_org_page_token)

    if not studies:
        print("No studies found on this page.")
        break

    for study in studies:
        ########################################## Parameterize Trial ##################################
        #print(varCurrentTrialLoop)
        varCurrentStudyProtocolSection = study.get("protocolSection")
        #print(str(varCurrentStudyProtocolSection))

        ######################################################################### IDENTIFICATION MODULE ###################################################################
        ########## IDENTIFICATION #########
        varCurrentStudyProtocolSectionIdentificationModule = varCurrentStudyProtocolSection.get("identificationModule")
        if varCurrentStudyProtocolSectionIdentificationModule:
            varCurrentStudyProtocolSectionIdentificationModuleNctId = varCurrentStudyProtocolSectionIdentificationModule.get("nctId")
            if clinicaltrias_gov_print:
                print(varCurrentStudyProtocolSectionIdentificationModuleNctId)

            varCurrentStudyProtocolSectionIdentificationModuleOfficialTitle = varCurrentStudyProtocolSectionIdentificationModule.get("officialTitle")
            if clinicaltrias_gov_print:
                print(varCurrentStudyProtocolSectionIdentificationModuleOfficialTitle)

            varCurrentStudyProtocolSectionIdentificationModuleBriefTitle = varCurrentStudyProtocolSectionIdentificationModule.get("briefTitle")
            if clinicaltrias_gov_print:
                print(varCurrentStudyProtocolSectionIdentificationModuleBriefTitle)

        ########## DESCRIPTION #########    
        varCurrentStudyProtocolSectionDescriptionModule = varCurrentStudyProtocolSection.get("descriptionModule")
        if varCurrentStudyProtocolSectionDescriptionModule:
            varCurrentStudyProtocolSectionDescriptionModuleDetailedDesctiption = varCurrentStudyProtocolSectionDescriptionModule.get("detailedDescription")
            if clinicaltrias_gov_print:
                print(varCurrentStudyProtocolSectionDescriptionModuleDetailedDesctiption)

        ######################################################################### Eligibility Module ###################################################################
        varCurrentStudyProtocolSectionEligibilityModule = varCurrentStudyProtocolSection.get("eligibilityModule")
        if varCurrentStudyProtocolSectionEligibilityModule:
            varCurrentStudyProtocolSectionEligibilityModuleEligibilityCriteria = varCurrentStudyProtocolSectionEligibilityModule.get("eligibilityCriteria")
            eligibility_criteria_lower = varCurrentStudyProtocolSectionEligibilityModuleEligibilityCriteria.lower()
            varTransformEligibilityInclusionCriteriaLocation = [i for i in range(len(eligibility_criteria_lower)) if eligibility_criteria_lower.startswith("inclusion criteria:", i)]
            varTransformEligibilityExclusionCriteriaLocation = [i for i in range(len(eligibility_criteria_lower)) if eligibility_criteria_lower.startswith("exclusion criteria:", i)]

            if varTransformEligibilityInclusionCriteriaLocation and varTransformEligibilityExclusionCriteriaLocation:
                varTransformInclusionCriteriaLowerList = eligibility_criteria_lower.split("exclusion criteria:")
                varTransformInclusionCriteriaRemoveHeader = varTransformInclusionCriteriaLowerList[0].replace("inclusion criteria:", "")
                varTransformInclusionCriteriaClean = varTransformInclusionCriteriaRemoveHeader.strip()
                varTransformExclusionCriteriaClean = varTransformInclusionCriteriaLowerList[1].strip()
                # Print inclusion and exclusion criteria
                if clinicaltrias_gov_print:
                    print(f"Inclusion Criteria:\n {varTransformInclusionCriteriaClean}")
                    print(f"Exclusion Criteria:\n {varTransformExclusionCriteriaClean}\n")

            varCurrentStudyProtocolSectionEligibilityModuleSex = varCurrentStudyProtocolSectionEligibilityModule.get("sex")
            if clinicaltrias_gov_print:
                print(varCurrentStudyProtocolSectionEligibilityModuleSex)

            varCurrentStudyProtocolSectionEligibilityModuleMinimumAge = varCurrentStudyProtocolSectionEligibilityModule.get("minimumAge")
            if clinicaltrias_gov_print:
                print(varCurrentStudyProtocolSectionEligibilityModuleMinimumAge)

            varCurrentStudyProtocolSectionEligibilityModuleMaximumAge = varCurrentStudyProtocolSectionEligibilityModule.get("maximumAge")
            if clinicaltrias_gov_print:
                print(varCurrentStudyProtocolSectionEligibilityModuleMaximumAge)

            varCurrentStudyProtocolSectionEligibilityModuleHealthyVolunteers = varCurrentStudyProtocolSectionEligibilityModule.get("healthyVolunteers")
            if clinicaltrias_gov_print:
                print(varCurrentStudyProtocolSectionEligibilityModuleHealthyVolunteers)

        ########################################################################### CONDITIONS MODULE #####################################################################
        varCurrentStudyProtocolSectionConditionsModule = varCurrentStudyProtocolSection.get("conditionsModule")
        if varCurrentStudyProtocolSectionConditionsModule:
            varCurrentStudyProtocolSectionConditionsModuleConditionList = varCurrentStudyProtocolSectionConditionsModule.get("conditions")
            varCurrentStudyProtocolSectionConditionsModuleConditionListLength = len(varCurrentStudyProtocolSectionConditionsModuleConditionList)
            if clinicaltrias_gov_print:
                print(varCurrentStudyProtocolSectionConditionsModuleConditionList)

        ############################################################ RUN AI ############################################################

        ############################################################ Azure Key Vault ############################################################
        print("Authenticating to Azure Key Vault.")
        # Azure Key Vault URL, Azure AD credentials

        key_vault_client_id = os.getenv("AZURE_CLIENT_ID")
        key_vault_client_secret = os.getenv("AZURE_CLIENT_SECRET")
        tenant_id = os.getenv("AZURE_TENANT_ID")

        # Authenticate to Azure
        credential = ClientSecretCredential(tenant_id, key_vault_client_id, key_vault_client_secret)

        # Connect to the Azure Key Vault
        client = SecretClient(vault_url=azure_key_vault_url, credential=credential)

        # Retrieve a secret and extract the secret value
        sql_client_id_secret_name = "SQL-Client-ID"
        sql_client_id_secret = client.get_secret(sql_client_id_secret_name)
        sql_client_id = sql_client_id_secret.value  # Extract the value

        sql_client_id_secret_name = "SQL-Client-Secret"
        sql_client_secret_secret = client.get_secret(sql_client_id_secret_name)
        sql_client_secret = sql_client_secret_secret.value  # Extract the value

        sql_tenant_id_secret_name = "SQL-Tenant-ID"
        sql_tenant_id_secret = client.get_secret(sql_client_id_secret_name)
        sql_tenant_id = sql_client_secret_secret.value  # Extract the value

        text_analytics_for_health_key_secret_name = "Text-Analytics-for-Health-Key"
        text_analytics_for_health_key_secret = client.get_secret(text_analytics_for_health_key_secret_name)
        text_analytics_for_health_key = text_analytics_for_health_key_secret.value  # Extract the value

        print("Keys Retrieved.")
        
        ############################################################ Text Analytics for Health ############################################################
        if run_ai:
            print("Authenticating to Text Analytics for Health.")
            # Your Azure Cognitive Services API key and endpoint

            # The specific path of the service you want to use (update as needed)
            path = 'text/analytics/v3.1/entities/health/jobs'

            # Full URL
            url = text_analytics_endpoint + path

            # Prepare the headers
            headers = {
                'Ocp-Apim-Subscription-Key': text_analytics_for_health_key,
                'Content-Type': 'application/json'
            }

            # Prepare the body of the request
            body = {
                "documents": [
                    {"id": "1", "language": "en", "text": varTransformInclusionCriteriaClean},
                    {"id": "2", "language": "en", "text": varTransformExclusionCriteriaClean},
                    {"id": "3", "language": "en", "text": str(varCurrentStudyProtocolSectionConditionsModuleConditionList)}
                ],
                "tasks": {
                    "kind": "Healthcare",
                    "parameters": {
                        "modelVersion": "2022-08-15-preview",
                        "fhirVersion": "5.0.1"
                    }
                }
            }

            # Make the POST request
            response = requests.post(url, headers=headers, json=body)
            response_json = None
            if response.status_code == 200:
                try:
                    response_json = response.json()
                    response_jsondumps = json.dumps(response_json)
                    #print(response_jsondumps)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    print(f"Response content: {response.text}")
            elif response.status_code == 202:
                print("Request accepted, processing...")
                operation_location = response.headers.get('operation-location')  # Get the operation location URL from the response headers

                if operation_location:
                    while True:
                        status_response = requests.get(operation_location, headers=headers)
                        status_response_json = status_response.json()

                        # Check the job status and proceed accordingly
                        job_status = status_response_json.get('status')
                        if job_status == 'succeeded':
                            print("Processing complete.")
                            break  # Exit the loop once processing is complete
                        elif job_status == 'failed':
                            print("Processing failed.")
                            print(json.dumps(status_response_json, indent=4))
                            break  # Exit the loop if processing failed

                        print("Still processing... Waiting before checking again.")
                        time.sleep(1)  # Wait for some time before checking the status again
                else:
                    print("Operation-location URL not found in response headers.")
            else:
                print(f"Error: {response.status_code}")

            status_response = requests.get(operation_location, headers=headers)
            response_json_dumps = status_response.json()

            # Process the sample data
            doc_str = str(response_json_dumps)
            json_string_fixed = doc_str.replace("'", '"')
            doc_str_json = json.loads(json_string_fixed)

            inclusion_criteria_entities = doc_str_json["results"]["documents"][0]["entities"]
            exclusion_criteria_entities = doc_str_json["results"]["documents"][1]["entities"]
            conditions_list_entities = doc_str_json["results"]["documents"][2]["entities"]
            print(doc_str_json["results"]["documents"][0])

        if write_to_db:
            authority = f'https://login.microsoftonline.com/{tenant_id}'
            resource = 'https://database.windows.net/'
            db_server = 'dev-worldtrials-sql.database.windows.net'
            db_name = 'TRIALS'

            app = ConfidentialClientApplication(sql_client_id, authority=authority, client_credential=sql_client_secret)
            token_response = app.acquire_token_for_client(scopes=[resource + '/.default'])
            #print(token_response)
            access_token = token_response['access_token']
            #print("SQL CLIENTID : " + sql_client_id)
            connection_string = (
                "Driver={ODBC Driver 18 for SQL Server};"
                "Server=tcp:worldtrials-dev-basic.database.windows.net,1433;"
                "Database=TRIALS;"
                "Uid=wtadmin;"
                "Pwd=WorldTrials1;"
                "Encrypt=yes;"
                "TrustServerCertificate=no;"
                "Connection Timeout=30;"
            )

            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()

            trials_table_query = """INSERT INTO [dbo].[LOAD_CLINICAL_TRIALS_TRIALS] (NCT_ID, OFFICIAL_TITLE, DETAILED_DESCRIPTION, CONDITIONS) VALUES (?, ?, ?, ?);"""
            trials_table_params = (varCurrentStudyProtocolSectionIdentificationModuleNctId, varCurrentStudyProtocolSectionIdentificationModuleOfficialTitle, varCurrentStudyProtocolSectionDescriptionModuleDetailedDesctiption, str(', '.join(varCurrentStudyProtocolSectionConditionsModuleConditionList)))
            cursor.execute(trials_table_query, trials_table_params)
            cursor.execute("SELECT @@IDENTITY AS ID;")
            trials_table_pk = cursor.fetchone()[0]
            cursor.commit()

            varCurrentStudyProtocolSectionConditionsModuleConditionConditionLoop = 0
            for varCurrentStudyProtocolSectionConditionsModuleConditionConditionLoop in range(varCurrentStudyProtocolSectionConditionsModuleConditionListLength):
                varCurrentStudyProtocolSectionConditionsModuleConditionConditionCondition = varCurrentStudyProtocolSectionConditionsModuleConditionList[varCurrentStudyProtocolSectionConditionsModuleConditionConditionLoop]

                cursor.execute("INSERT INTO [dbo].[LOAD_CLINICAL_TRIALS_CONDITION] (NCT_ID, TRIAL_ID, CONDITION) VALUES (?,?,?)", (varCurrentStudyProtocolSectionIdentificationModuleNctId, trials_table_pk, varCurrentStudyProtocolSectionConditionsModuleConditionConditionCondition))
                cursor.execute("SELECT @@IDENTITY AS ID;")
                condition_table_pk = cursor.fetchone()[0]
                cursor.commit()

                varCurrentStudyProtocolSectionConditionsModuleConditionConditionLoop += 1
            
            if run_ai_enhancement:
                # Prepare the system prompt
                system_prompt_content = """
                You are a clinical trial summarization expert. Do not include any extraneous conversational material, affirmations, or peripheral content. Only respond with the requested data in Markdown format. You will be provided with three sections from clinical trials: "Detailed Description", "Inclusion Criteria", and "Exclusion Criteria". Your task is to summarize these sections for a 16-year-old audience. Be concise and accurate. The Markdown returned should only use bullets and bolding - no other formatting elements. Bold only key words and phrases.

                For the "Detailed Description":

                - Mention the **treatment name**, **time period**, **risks**, **expected results**, and **why someone might want to join** the study.
                - The summary should be **no longer than two paragraphs**.

                Respond in this format for the "Detailed Description", which will now be called the **"Enhanced summary"**:

                Enhanced summary: {your enhanced summary in Markdown goes here}

                For the "Inclusion Criteria" and "Exclusion Criteria":

                - Summarize the criteria for a 16-year-old audience.
                - Use **bullet points**.
                - Be concise.

                Respond in these formats:

                Enhanced Inclusion criteria: {your enhanced inclusion criteria in Markdown goes here}
                Enhanced Exclusion criteria: {your enhanced exclusion criteria in Markdown goes here}

                **Important:** If any of the input sections ("Detailed Description", "Inclusion Criteria", or "Exclusion Criteria") are missing, empty, or contain null values, output **null** (without quotes) for that section and do not generate any content for it.

                Do not include any other text in your response.
                """



                if varCurrentStudyProtocolSectionDescriptionModuleDetailedDesctiption is None:
                    AiTrialDescription = varCurrentStudyProtocolSectionIdentificationModuleBriefTitle
                else:
                    AiTrialDescription = varCurrentStudyProtocolSectionDescriptionModuleDetailedDesctiption

                system_prompt = {
                    "role": "system",
                    "content": system_prompt_content
                }

                # Prepare the user message with the variables
                user_content = f"""
                Detailed description:
                {AiTrialDescription}

                Inclusion criteria:
                {varTransformInclusionCriteriaClean}

                Exclusion criteria:
                {varTransformExclusionCriteriaClean}
                """

                messages = [
                    system_prompt,
                    {"role": "user", "content": user_content}
                ]

                # Call the OpenAI API
                try:
                    response = enhancementclient.chat.completions.create(
                        model="gpt-4o",
                        messages=messages
                    )
                    enhanced_text = response.choices[0].message.content

                    # Extract the enhanced sections using regex
                    import re
                    enhanced_description_match = re.search(
                        r'Enhanced summary:\s*(.*?)\s*(?=Enhanced Inclusion criteria:|$)',
                        enhanced_text, re.DOTALL)
                    enhanced_inclusion_match = re.search(
                        r'Enhanced Inclusion criteria:\s*(.*?)\s*(?=Enhanced Exclusion criteria:|$)',
                        enhanced_text, re.DOTALL)
                    enhanced_exclusion_match = re.search(
                        r'Enhanced Exclusion criteria:\s*(.*)', enhanced_text, re.DOTALL)

                    enhanced_description = enhanced_description_match.group(1).strip() if enhanced_description_match else ''
                    enhanced_inclusion = enhanced_inclusion_match.group(1).strip() if enhanced_inclusion_match else ''
                    enhanced_exclusion = enhanced_exclusion_match.group(1).strip() if enhanced_exclusion_match else ''

                    # Now you can proceed to use these enhanced summaries as needed
                    # For example, print them or store them in variables
                    if print_ai_enhancements:
                        print("Enhanced summary:")
                        print(enhanced_description)
                        print("\nEnhanced Inclusion criteria:")
                        print(enhanced_inclusion)
                        print("\nEnhanced Exclusion criteria:")
                        print(enhanced_exclusion)

                except Exception as e:
                    print("An error occurred while calling the OpenAI API:")
                    print(e)

            cursor.execute("INSERT INTO [dbo].[TRANSFORM_CLINICAL_TRIALS_ELIGIBILITY] (NCT_ID, TRIAL_ID, INCLUSION_CRITERIA, EXCLUSION_CRITERIA, ENHANCED_DESCRIPTION, ENHANCED_INCLUSION, ENHANCED_EXCLUSION) VALUES (?,?,?,?,?,?,?)", (varCurrentStudyProtocolSectionIdentificationModuleNctId, trials_table_pk, varTransformInclusionCriteriaClean, varTransformExclusionCriteriaClean,enhanced_description,enhanced_inclusion,enhanced_exclusion))
            cursor.execute("SELECT @@IDENTITY AS ID;")
            transform_eligibility_table_pk = cursor.fetchone()[0]
            cursor.commit()

            cursor.execute("INSERT INTO [dbo].[LOAD_CLINICAL_TRIALS_ELIGIBILITY] (NCT_ID, TRIAL_ID, GENDER, MINIMUM_AGE, MAXIMUM_AGE, HEALTHY_VOLUNTEERS) VALUES (?,?,?,?,?,?)", (varCurrentStudyProtocolSectionIdentificationModuleNctId, trials_table_pk, varCurrentStudyProtocolSectionEligibilityModuleSex, varCurrentStudyProtocolSectionEligibilityModuleMinimumAge, varCurrentStudyProtocolSectionEligibilityModuleMaximumAge, str(varCurrentStudyProtocolSectionEligibilityModuleHealthyVolunteers)))
            cursor.execute("SELECT @@IDENTITY AS ID;")
            eligibility_table_pk = cursor.fetchone()[0]
            cursor.commit()

        


        print("Trials Processed: " + str(varCurrentTrialLoop + 1))
        varCurrentTrialLoop += 1

    if not clinicaltrials_org_page_token:
        break  # Exit the loop if there are no more pages to fetch

    varCurrentPage += 1
