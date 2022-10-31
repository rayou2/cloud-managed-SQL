import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

#.env file login 
load_dotenv()
GCP_MYSQL_HOSTNAME = os.getenv("GCP_MYSQL_HOSTNAME")
GCP_MYSQL_USER = os.getenv("GCP_MYSQL_USERNAME")
GCP_MYSQL_PASSWORD = os.getenv("GCP_MYSQL_PASSWORD")
GCP_MYSQL_DATABASE = os.getenv("GCP_MYSQL_DATABASE")

connection_string_gcp = f'mysql+pymysql://{GCP_MYSQL_USER}:{GCP_MYSQL_PASSWORD}@{GCP_MYSQL_HOSTNAME}:3306/{GCP_MYSQL_DATABASE}'
db_gcp = create_engine(connection_string_gcp)

#show databases 
tableNames_gcp = db_gcp.table_names()
print(db_gcp.table_names)

# creating tables in patients db for: patients, medications, 
# treatment_procedures, conditions, and social determinants

table_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_medications = """
create table if not exists medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
); 
"""
table_patients_medications = """
create table if not exists patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    med_ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES medications(med_ndc) ON DELETE CASCADE
); 
"""

table_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

table_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE
); 
"""
table_treatment = """
create table if not exists treatment (
    id int auto_increment,
    treat_cpt varchar(255) default null unique,
    treat_human_name varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

table_patient_treatment = """
create table if not exists patient_treatment (
    id int auto_increment,
    mrn varchar(255) default null,
    treat_cpt varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (treat_cpt) REFERENCES treatment(treat_cpt) ON DELETE CASCADE
); 
"""

table_social = """
create table if not exists social (
    id int auto_increment,
    social_lonic varchar(255) default null unique,
    social_human_name varchar(255) default null,
    PRIMARY KEY (id)
); 
""" 

table_social_patient = """
create table if not exists patient_social (
    id int auto_increment,
    mrn varchar(255) default null,
    social_lonic varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (social_lonic) REFERENCES social(social_lonic) ON DELETE CASCADE
); 
"""

db_gcp.execute(table_patients)
db_gcp.execute(table_medications)
db_gcp.execute(table_patients_medications)
db_gcp.execute(table_conditions)
db_gcp.execute(table_patient_conditions)
db_gcp.execute(table_treatment)
db_gcp.execute(table_patient_treatment)
db_gcp.execute(table_social)
db_gcp.execute(table_social_patient)

print(db_gcp.table_names())