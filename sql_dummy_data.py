from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
from faker import Faker 
import uuid
import random
import dbm

#.env file login 
load_dotenv()
GCP_MYSQL_HOSTNAME = os.getenv("GCP_MYSQL_HOSTNAME")
GCP_MYSQL_USER = os.getenv("GCP_MYSQL_USERNAME")
GCP_MYSQL_PASSWORD = os.getenv("GCP_MYSQL_PASSWORD")
GCP_MYSQL_DATABASE = os.getenv("GCP_MYSQL_DATABASE")
connection_string_gcp = f'mysql+pymysql://{GCP_MYSQL_USER}:{GCP_MYSQL_PASSWORD}@{GCP_MYSQL_HOSTNAME}:3306/{GCP_MYSQL_DATABASE}'
db_gcp = create_engine(connection_string_gcp)

fake = Faker()
fake_patients = [
    {
        #keep just the first 8 characters of the uuid
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number()
    } for x in range(50)]
df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicate mrn
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])


#### real icd10 codes
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)
# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')
print(icd10codesShort_1k)

#### real ndc codes
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

### real cpt codes
cpt_codes = pd.read_csv('https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv')
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
cpt_codes_1k = cpt_codes_1k.drop_duplicates(subset=['com.medigy.persist.reference.type.clincial.CPT.code'], keep='first')

### real lonic codes
lonic_codes = pd.read_csv('https://raw.githubusercontent.com/elleros/DSHealth2019_loinc_embeddings/master/Data/coh_top_500_lab_cats_cs.csv')
lonic_codes_1k = lonic_codes.sample(n=400, random_state=1)
lonic_codes_1k = lonic_codes_1k.drop_duplicates(subset=['Loinc'], keep='first')

#patient info
insertQuery = "INSERT INTO patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile) VALUES (%s, %s, %s, %s, %s, %s, %s)"
for index, row in df_fake_patients.iterrows():
    db_gcp.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'], row['gender'], row['contact_mobile']))
    print("inserted row: ", index)
# # query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM patients", db_gcp)

#conditition info 
insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"
startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_gcp.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_gcp: ", index)
    if startingRow == 100:
        break
df_gcp = pd.read_sql_query("SELECT * FROM conditions", db_gcp)

#medication info
insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"
medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    # db_azure.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    db_gcp.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if medRowCount == 75:
        break
df_gcp = pd.read_sql_query("SELECT * FROM medications", db_gcp)

#treatment info
insertQuery = "INSERT INTO treatment (treat_cpt, treat_human_name) VALUES (%s, %s)"
treatRowCount = 0
for index, row in cpt_codes_1k.iterrows():
    treatRowCount += 1
    db_gcp.execute(insertQuery, (row['com.medigy.persist.reference.type.clincial.CPT.code'], row['label']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if treatRowCount == 75:
        break
df_gcp = pd.read_sql_query("SELECT * FROM treatment", db_gcp)

#social info 
insertQuery = "INSERT INTO social (social_lonic, social_human_name) VALUES (%s, %s)"
lonicRowCount = 0
for index, row in lonic_codes_1k.iterrows():
    lonicRowCount += 1
    db_gcp.execute(insertQuery, (row['Loinc'], row['Category']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if lonicRowCount == 100:
        break
df_gcp = pd.read_sql_query("SELECT * FROM social", db_gcp)

#first, lets query production_conditions and production_patients to get the ids
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", db_gcp)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_gcp)

# create a dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random number of conditions between 1 and 5
    # numConditions = random.randint(1, 5)
    # get a random sample of conditions from df_conditions
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)
print(df_patient_conditions.head(20))
# now lets add a random condition to each patient
insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"
for index, row in df_patient_conditions.iterrows():
    db_gcp.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)

#fake 
df_medications = pd.read_sql_query("SELECT med_ndc FROM medications", db_gcp) 
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_gcp)

# create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc'])
# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 5
    numMedications = random.randint(1, 5)
    # get a random sample of medications from df_medications
    df_medications_sample = df_medications.sample(n=numMedications)
    # add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
    df_patient_medications = df_patient_medications.append(df_medications_sample)

print(df_patient_medications.head(10))

# now lets add a random medication to each patient
insertQuery = "INSERT INTO patient_medications (mrn, med_ndc) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db_gcp.execute(insertQuery, (row['mrn'], row['med_ndc']))
    print("inserted row: ", index)