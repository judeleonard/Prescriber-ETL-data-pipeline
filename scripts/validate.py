import logging
from pyspark.sql.functions import count
import pandas



def df_count(df,dfName):
    """This function validates the ingested data for the city dimension data and usa_prescriber data"""
    try:
        logging.info(f"The DataFrame Validation by count df_count() is started for Dataframe {dfName}...")
        df_count=df.count()
        logging.info(f"The DataFrame count is {df_count}.")
    except Exception as exp:
        logging.error("Error in the method - df_count(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logging.info(f"The DataFrame Validation by count df_count() is completed.")


def df_print_schema(df,dfName):
    try:
        logging.info(f"The DataFrame Schema Validation for Dataframe {dfName}...")
        sch=df.schema.fields
        logging.info(f"The DataFrame {dfName} schema is: ")
        for i in sch:
            logging.info(f"\t{i}")
    except Exception as exp:
        logging.error("Error in the method - df_show_schema(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logging.info("The DataFrame Schema Validation is completed.")


def df_top10_rec(df,dfName):
    try:
        logging.info(f"The DataFrame Validation by top 10 record df_top10_rec() is started for Dataframe {dfName}...")
        logging.info(f"The DataFrame top 10 records are:.")
        df_pandas=df.limit(10).toPandas()
        logging.info('\n \t'+ df_pandas.to_string(index=False))
    except Exception as exp:
        logging.error("Error in the method - df_top10_rec(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        logging.info("The DataFrame Validation by top 10 record df_top10_rec() is completed.")