#This Python script constitutes a data pipeline to load data from local and perform ETL transformations on the data and dump them to data warehouse
#This code is submitted by:
#1. Subhram Satyajeet 110127932
#2. Parneet Kaur - 110124612
#3. Pooja Vishwakarma - 110124478
#4. Himadhar Reddy Mareddy - 110128616

#########################################################start of the script########################################################################

#Importing the libraries
#Google authentication libraries for different services
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
import os
import csv


#Setting the service account credentials for authentication
SERVICE_ACCOUNT_FILE = 'amplified-brook-416922-3adbaaae0c2a.json' #key file name
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE) #setting credentials using key file

#This function reads the column names from each csv and returns them to caller
def get_cols_list(source_file_name):
    
    #read column names from local csv files
    with open(source_file_name, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=',')
        headers = reader.fieldnames
    
    #return column names as headere
    return headers

#Bronze layer ingestion
#This function loads the data from local filesystem to google cloud bucket
def upload_files(source_file_name):
    #setting the destination file name for bucket
    destination_blob_name = source_file_name
    #authenticating client for bucket
    storage_client = storage.Client(project='amplified-brook-416922' ,credentials = credentials)
    #setting the bucket name
    bucket = storage_client.bucket('adt-project-gender-equality')
    blob = bucket.blob(source_file_name)
    #upload the file to bucket
    blob.upload_from_filename(destination_blob_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

#End of the bronze layer section

#silver section ingestion
#This function creates the external tables on top of gcp bucket files
def create_external_tables(source_file_name, cols_list):
    # Constructing a BigQuery client object and authentication
    bigquery_client = bigquery.Client(project='amplified-brook-416922' ,credentials = credentials)
    source_csv_name = source_file_name
    #mapping file names to table names as per the syntax of bigquery
    source_file_name = source_file_name.lower().replace(" ","_")
    source_file_name = source_file_name.lower().replace("-","_")
    
    #constructing table creation statement using google sql
    create_table_ddl = """create or replace external table amplified-brook-416922.adt_data_warehouse.{0} (""".format(source_file_name)
    #adding columns of table
    for col_name in cols_list:
        create_table_ddl = create_table_ddl + " " + col_name.lower().replace(" ","_") + " STRING, "
    
    create_table_ddl.rstrip(", ")
    #setting the location of the data using gcp uri
    create_table_ddl = create_table_ddl + ") OPTIONS (format = 'CSV', uris = ['gs://adt-project-gender-equality/"
    create_table_ddl = create_table_ddl + source_csv_name +".csv'], skip_leading_rows = 1 )"
    print(create_table_ddl)
    #creation of external table
    res = bigquery_client.query_and_wait(create_table_ddl)

###End of silver layer section

##Gold layer ingestion section
def etl_table_formation_step():
    # Constructing a BigQuery client object.
    bigquery_client = bigquery.Client(project='amplified-brook-416922' ,credentials = credentials)
    
    #Listing all tables in the silver schema
    tables = bigquery_client.list_tables("adt_data_warehouse")  # Make an API request.
    col_rep_check_ind = 0
    col_rep_check_country = 0
    col_rep_check_year = 0
    #creation of fact table ddl statement
    create_fact_table_ddl = """CREATE OR REPLACE TABLE {0}.{1} (""".format('adt_data_modelling','gender_fact')
    print("Tables contained in '{}':".format("adt_data_warehouse"))
    
    #for each table 
    for table in tables:
        #create_fact_table_ddl = "";
        create_dim_table_ddl = "";
        
        
        #print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        ##extract column names and data types
        fetch_column_info_ddl = """SELECT column_name, data_type FROM `{0}`.INFORMATION_SCHEMA.COLUMNS 
        WHERE
        table_name = '{1}';""".format('adt_data_warehouse',table.table_id)
        #print(fetch_column_info_ddl)
        res = bigquery_client.query_and_wait(fetch_column_info_ddl)
        #creation of dimension table ddl statement
        create_dim_table_ddl = """CREATE OR REPLACE TABLE {0}.{1} (""".format('adt_data_modelling','gender_dimension_'+table.table_id)
        for row in res:
            #print(row[0]+" "+row[1])
        #####Fact table creation
            if col_rep_check_ind == 0:
                if row[0] in ['indicator_code']:
                    create_fact_table_ddl = create_fact_table_ddl + row[0] +" "+row[1]+", " 
                    col_rep_check_ind = 1
            elif col_rep_check_country == 0:
                if row[0] in ['country_code']:
                    create_fact_table_ddl = create_fact_table_ddl + row[0] +" "+row[1]+", " 
                    col_rep_check_country = 1
            elif col_rep_check_year == 0:
                if row[0] in ['year']:
                    create_fact_table_ddl = create_fact_table_ddl + row[0] +" "+row[1]+", " 
                    col_rep_check_year = 1
            else:
                if row[0] in ['value']:
                    create_fact_table_ddl = create_fact_table_ddl + row[0] + "_" + table.table_id +" STRING DEFAULT '0', "
            
            if row[0] not in ['year', 'value']:
                create_dim_table_ddl = create_dim_table_ddl + row[0] +" "+row[1]+", "
            
        create_dim_table_ddl = create_dim_table_ddl +");"
        print(create_dim_table_ddl)
        #creation of dimension table using formed ddl
        res = bigquery_client.query_and_wait(create_dim_table_ddl)
            
    create_fact_table_ddl = create_fact_table_ddl +");"
    print(create_fact_table_ddl)
    #creation of fact table using formed ddl
    res = bigquery_client.query_and_wait(create_fact_table_ddl)
    

#This function loads the data into fact and dimension tables constructed by previous function
def etl_data_insertion_loader():
    # Construct a BigQuery client object
    bigquery_client = bigquery.Client(project='amplified-brook-416922' ,credentials = credentials)
    #List all tables in the silver schema
    tables = bigquery_client.list_tables("adt_data_warehouse")  # Make an API request.
    first_run = 0
    print("Tables contained in '{}':".format("adt_data_warehouse"))
    counter = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    i = 0
    
    #construct insert table statement for each table and execute
    for table in tables:
        #construction of insert statement for dimension table
        insert_dim_data = """insert into {0}.{1}  select """.format('adt_data_modelling', 'gender_dimension_'+table.table_id)
        #contruction of insert statement for fact table
        insert_fact_data = """insert into {0}.{1}  select """.format('adt_data_modelling', 'gender_fact')
        counter[i] = 1
        #getting column names from data warehouse tables
        fetch_column_info_ddl = """SELECT column_name, data_type FROM `{0}`.INFORMATION_SCHEMA.COLUMNS 
        WHERE
        table_name = '{1}';""".format('adt_data_warehouse',table.table_id)
        #print(fetch_column_info_ddl)
        res = bigquery_client.query_and_wait(fetch_column_info_ddl)
        #selecting column names based on the conditionals
        if first_run == 1:
            insert_fact_data = insert_fact_data +" a.indicator_code, a.country_code, "
        for row in res:
            if first_run == 0:
                if row[0] not in ['country_name', 'indicator_name', 'value']:
                    insert_fact_data = insert_fact_data +"a." +row[0] +", "
                    
                
                else:   
                    #insert_dim_data = insert_dim_data + row[0] +", "
                    pass
                    
                
                #insert_dim_data = insert_dim_data + "country_code, indicator_code"
                
            else:
                if row[0] in ['country_name', 'country_code', 'indicator_name', 'indicator_code']:
                    insert_dim_data = insert_dim_data + row[0] +", "
                    
                else:
                    if row[0] in ['country_code', 'indicator_code', 'year']:
                        insert_fact_data = insert_fact_data + "a." +row[0] +", "
                    
        if first_run == 0:
            insert_dim_data = insert_dim_data + " indicator_name, indicator_code, country_name, country_code "
        
        
        for j in range (0,14):
            if counter[j] == 1:
                insert_fact_data = insert_fact_data + " value, "
            else :
                insert_fact_data = insert_fact_data + "'0', "
        insert_dim_data = insert_dim_data + """ from {0}.{1}.{2} """.format(table.project, table.dataset_id, table.table_id) 
        insert_fact_data = insert_fact_data + """ from {0}.{1}.{2} a""".format(table.project, table.dataset_id, table.table_id)
        
        #include certain columns only for first run
        if first_run ==1:
            insert_fact_data = insert_fact_data + """ left join {0}.{1}.{2} b 
                                on a.country_code = b.country_code 
                                and a.indicator_code = b.indicator_code 
                                where a.indicator_code is NULL""".format(table.project, 'adt_data_modelling', 'gender_fact') 
        print('---------------------------------------------------------------------------------------------------------')
        
        print(insert_dim_data)
        print(insert_fact_data)
        #insertion of data into dimension table
        res = bigquery_client.query_and_wait(insert_dim_data)
        #insertion of data into fact table
        res = bigquery_client.query_and_wait(insert_fact_data)
        
        first_run = 1
        counter[i] = 0
        i = i+1
        

#this function updates the columns of certains rows of data for fact table that is already inserted to avoid duplication insertion of data
def etl_data_updation_loader():
    # Construction a BigQuery client object.
    bigquery_client = bigquery.Client(project='amplified-brook-416922' ,credentials = credentials)
    #List all tables in the silver schema
    tables = bigquery_client.list_tables("adt_data_warehouse")  # Make an API request.
    print("Tables contained in '{}':".format("adt_data_warehouse"))
    
    for table in tables:
        #constructing update table statement 
        update_fact_data = """update  {0}.{1} a  set a.value_{2} = b.value""".format('adt_data_modelling', 'gender_fact', table.table_id)

        #update_fact_data = update_fact_data + """ from {0}.{1}.{2} """.format(table.project, table.dataset_id, 'gender_fact')


        update_fact_data = update_fact_data + """ from {0}.{1}.{2} b 
                        where a.country_code = b.country_code 
                        and a.indicator_code = b.indicator_code 
                        and a.year = b.year
                        and a.indicator_code is NOT NULL""".format(table.project, table.dataset_id, table.table_id) 
        print('---------------------------------------------------------------------------------------------------------')

        print(update_fact_data)
        #execution of update table statement
        res = bigquery_client.query_and_wait(update_fact_data)
        
 
#End of gold layer ingestion section 


            
#main code functionality

for source_file_name in os.listdir():
        if source_file_name.endswith(".csv"):
            print(source_file_name)
            cols_list = []
            #step1 upload from local to bucket
            upload_files(source_file_name)
            
            #creation of external table 
            cols_list = get_cols_list(source_file_name)
            print(cols_list)
            
            create_external_tables((source_file_name).split('.')[0], cols_list);
            
            #create data warehouse table
            
#creation of data warehouse tables
etl_table_formation_step()  

#insertion of data into warehouse tables 
etl_data_insertion_loader() 

#updation of data in the warehouse tables            
etl_data_updation_loader()


###############################################################end of the script############################################################################
