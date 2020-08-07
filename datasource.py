from pyspark import sql as spark_sql
from pyspark.sql.functions import lit


class SparkInput():

    def __init__(self, spark_session, spark_dataframe: spark_sql.DataFrame, temp_table_name = "filter_dataframe",validation= False, error_column = 'error_messages'):
        self.spark = spark_session
        self.dataframe = spark_dataframe
        self.temp_table_name = temp_table_name
        self.columns = self.dataframe.schema.names
        self.dataframe = self.dataframe.withColumn(error_column, lit(""))

        self.validate_dataframe = validation        
        if self.validate_dataframe:
            self.validate_spark_dataframe(self.dataframe)

    def validate_spark_dataframe(self,spark_dataframe: spark_sql.DataFrame):
        number_of_rows = spark_dataframe.count().collect()[0][0]
        if number_of_rows == 0:
            raise ValueError("The Dataframe is empty")
    
    def sql_method(self, query:str, spark_dataframe: spark_sql.DataFrame):
        spark_dataframe.createOrReplaceTempView(self.temp_table_name)
        return self.spark.sql(query)



class ErrorTesting():
    
    def __init__(self, table_name:str ):
        self.table_name = table_name

    def sql_method(self,query):
        pass

    def check_null(self,column_name:str,message:str):
        pass

    def check_boundarie(self, column_name: str, message: str, boundarie):
        pass

    def check_numerical_string(self, column_name: str, message: str):
        pass
    
    def check_cardinality(self, column_name: str, message: str):
        pass

    def check_with_case_query(self, column_name: str, case_query: str):
        pass