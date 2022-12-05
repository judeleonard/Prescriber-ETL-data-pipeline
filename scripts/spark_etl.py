from pyspark.sql import SparkSession
from pyspark.sql.functions import (upper,size, countDistinct, sum, dense_rank, col, 
                                   lit, regexp_extract, 
                                   concat_ws, count, isnan, 
                                   when,  avg, round, coalesce)
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import logging
import sys 
sys.path.append('./')
from validate import df_count, df_print_schema, df_top10_rec



# create spark session state
def spark_session():
    appName = "spark etl pipeline"
    spark = SparkSession.builder \
        .appName(appName) \
        .getOrCreate()
    
    return spark


def load_city_dim_table(spark, file_dir, file_format):
    try:
        logging.info("load_city_file() is Started ...")
        df = spark. \
                read. \
                format(file_format). \
                load(file_dir)
    except Exception as e:
        logging.error("Error in the method - load_table()). Please check the Stack Trace. " + str(e))
        raise
    else:
        logging.info(f"The input File {file_dir} is loaded to the data frame. The load_city_table() Function is completed.")
    return df


def load_presc_fact_table(spark, file_dir, file_format):
    try:
        logging.info("load_presc_fact_table() is Started ...")
        df = spark. \
            read. \
            format(file_format). \
            options(header=True). \
            options(inferSchema=True). \
            options(delimiter=','). \
            load(file_dir)
    except Exception as exp:
        logging.error("Error in the method - load_presc_fact_table(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logging.info(f"The input File {file_dir} is loaded to the data frame. The load_presc_fact_table() Function is completed.")
    return df


@udf(returnType=IntegerType())
def column_split_cnt(column):
    return len(column.split(' '))


def perform_data_clean(df1,df2):
    ### Clean df_city DataFrame:
    #1 Select only required Columns
    #2 Convert city, state and county fields to Upper Case
    try:
        logging.info(f"perform_data_clean() is started for df_city dataframe...")
        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips)

    ### Clean df_fact DataFrame:
    #1 Select only required Columns
    #2 Rename the columns
        logging.info(f"perform_data_clean() is started for df_fact dataframe...")
        df_fact_sel = df2.select(df2.npi.alias("presc_id"),df2.nppes_provider_last_org_name.alias("presc_lname"), \
                             df2.nppes_provider_first_name.alias("presc_fname"),df2.nppes_provider_city.alias("presc_city"), \
                             df2.nppes_provider_state.alias("presc_state"),df2.specialty_description.alias("presc_spclt"), df2.years_of_exp, \
                             df2.drug_name,df2.total_claim_count.alias("trx_cnt"),df2.total_day_supply, \
                             df2.total_drug_cost)
    #3 Add a Country Field 'USA'
        df_fact_sel = df_fact_sel.withColumn("country_name",lit("USA"))

    #4 Clean years_of_exp field
        pattern='\d+'
        idx=0
        df_fact_sel = df_fact_sel.withColumn("years_of_exp",regexp_extract(col("years_of_exp"),pattern,idx))
    #5 Convert the yearS_of_exp datatype from string to Number
        df_fact_sel = df_fact_sel.withColumn("years_of_exp",col("years_of_exp").cast("int"))

    #6 Combine First Name and Last Name
        df_fact_sel = df_fact_sel.withColumn("presc_fullname",concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")

    #8 Delete the records where the PRESC_ID is NULL
        df_fact_sel = df_fact_sel.dropna(subset="presc_id")

    #9 Delete the records where the DRUG_NAME is NULL
        df_fact_sel = df_fact_sel.dropna(subset="drug_name")

    #10 Impute TRX_CNT where it is null as avg of trx_cnt for that prescriber
        spec = Window.partitionBy("presc_id")
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', coalesce("trx_cnt",round(avg("trx_cnt").over(spec))))
        df_fact_sel=df_fact_sel.withColumn("trx_cnt",col("trx_cnt").cast('integer'))


    except Exception as exp:
        print(exp)
        raise
    else:
        logging.info("perform_data_clean() is completed...")
    return df_city_sel,df_fact_sel



def city_report(df_city_sel, df_fact_sel):
    """
    City Report:
       Transform Logics:
       1. Calculate the Number of zips in each city.
       2. Calculate the number of distinct Prescribers assigned for each City.
       3. Calculate total TRX_CNT prescribed for each city.
       4. Do not report a city in the final report if no prescriber is assigned to it.

    Layout:
       City Name
       State Name
       County Name
       City Population
       Number of Zips
       Prescriber Counts
       Total Trx counts
    """
    try:
        logging.info(f"Transform - city_report() is started...")
        load_path = '/opt/airflow/staging/city.csv'
        df_city_split = df_city_sel.withColumn('zip_counts',column_split_cnt(df_city_sel.zips))
        df_fact_grp = df_fact_sel.groupBy(df_fact_sel.presc_state, df_fact_sel.presc_city).agg(countDistinct("presc_id").alias("presc_counts"), sum("trx_cnt").alias("trx_counts"))
        df_city_join = df_city_split.join(df_fact_grp,(df_city_split.state_id == df_fact_grp.presc_state) & (df_city_split.city == df_fact_grp.presc_city),'inner')
        df_city_final = df_city_join.select("city","state_name","county_name","population","zip_counts","trx_counts","presc_counts")
        #df_city_final.write.mode('overwrite').parquet(load_path)
        df_city_final.toPandas().to_csv(load_path)
    except Exception as exp:
        logging.error("Error in the method - city_report(). Please check the Stack Trace. " + str(exp),exc_info=True)
        raise
    else:
        logging.info("Transform - city_report() is completed...")
    return df_city_final



def top_5_Prescribers(df_fact_sel):
    """
    # Prescriber Report:
    Top 5 Prescribers with highest trx_cnt per each state.
    Consider the prescribers only from 20 to 50 years of experience.
    Layout:
      Prescriber ID
      Prescriber Full Name
      Prescriber State
      Prescriber Country
      Prescriber Years of Experience
      Total TRX Count
      Total Days Supply
      Total Drug Cost
    """
    try:
        logging.info("Transform - top_5_Prescribers() is started...")
        load_path = '/opt/airflow/staging/top5_prescribers.csv'
        spec = Window.partitionBy("presc_state").orderBy(col("trx_cnt").desc())
        df_presc_final = df_fact_sel.select("presc_id","presc_fullname","presc_state","country_name","years_of_exp","trx_cnt","total_day_supply","total_drug_cost") \
           .filter((df_fact_sel.years_of_exp >= 20) & (df_fact_sel.years_of_exp <= 50) ) \
           .withColumn("dense_rank",dense_rank().over(spec)) \
           .filter(col("dense_rank") <= 5) \
           .select("presc_id","presc_fullname","presc_state","country_name","years_of_exp","trx_cnt","total_day_supply","total_drug_cost")
        #df_presc_final.write.format("csv").options(header='True', delimiter=',').mode('overwrite').save(load_path)
        df_presc_final.toPandas().to_csv(load_path)
    except Exception as exp:
        logging.error("Error in the method - top_5_Prescribers(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logging.info("Transform - top_5_Prescribers() is completed...")
    return df_presc_final


def main():
    """base function that runs spark etl """
    try:
        spark = spark_session()
    
        city_dir = '/opt/airflow/data/us_cities_dimension.parquet'
        presc_dir = '/opt/airflow/data/USA_Presc_Medicare_Data_2021.csv'
        #file_dir = '/opt/airflow/data'

        df_city = load_city_dim_table(spark = spark, file_dir = city_dir, file_format = 'parquet')
        df_fact = load_presc_fact_table(spark = spark, file_dir = presc_dir, file_format = 'csv')
        # validate ingested data for the city_dimensions data and fact_prescriber's data
        df_count(df_city,'df_city')
        df_count(df_fact,'df_fact')
        ### Initiate presc_run_data_preprocessing Script
            ## Perform data Cleaning Operations for df_city and df_fact
        df_city_sel,df_fact_sel = perform_data_clean(df_city,df_fact)
        #Validation for df_city and df_fact    
        df_print_schema(df_city_sel,'df_city_sel')
        df_print_schema(df_fact_sel,'df_fact_sel')
        ### Initiate presc_run_data_transform Script and write output to local staging dir
        df_city_final = city_report(df_city_sel,df_fact_sel)
        #city_report(df_city_sel,df_fact_sel)
        df_presc_final = top_5_Prescribers(df_fact_sel)
        
        #top_5_Prescribers(df_fact_sel)
        #Validation for df_city_final
        df_print_schema(df_city_final,'df_city_final')
        df_count(df_city_final,'df_city_final')
        # Validate for presc_final
        df_top10_rec(df_presc_final,'df_presc_final')
        df_print_schema(df_presc_final,'df_presc_final')
        df_count(df_presc_final,'df_presc_final')

    except Exception as exp:
            logging.error("Error Occured in the main() method. check the Stack Trace to go to the respective module "
                "and fix it." +str(exp))
            sys.exit(1)




if __name__ == "__main__" :
    logging.info("spark_etl is Started ...")
    main()
