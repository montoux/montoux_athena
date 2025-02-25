import boto3
import time
import pandas as pd

class AthenaQuery:
    """
    Simple interface to execute Athena queries and get outputs into a
    Pandas dataframe. An Athena session can be started with the following
    parameters

    .. code ::

        aq = AthenaQuery(region_name='ap-southeast-2',
                 athena_database='my_database',
                 athena_output='s3://data.athena.datascience.aunz.montoux.com/')

    :param dict kwargs: arguments specific to Athena query (`region_name`, `athena_database`, `athena_output`)
    """
    
    def __init__(self, **kwargs):
        self.session = boto3.Session(region_name=kwargs.pop('region_name', None))
        self.client = self.session.client('athena')
        
        self.athena_database = kwargs.pop('athena_database', None)
        self.athena_output = kwargs.pop('athena_output', None)

        
    def run_query(self, sql_query):
        """
        Run an Athena query.

        :param str sql_query: The SQL query to send to Athena
        :return: The Athena execution ID
        :rtype: str
        """
        response = self.client.start_query_execution(
            QueryString=sql_query,
            QueryExecutionContext={
                'Database': self.athena_database
            },
            ResultConfiguration={
                'OutputLocation': self.athena_output
            })

        return response['QueryExecutionId']

    
    def get_query_status(self, execution_id, wait=False):
        """
        Get Athena query status.

        :param str execution_id: The Athena execution ID
        :return: The Athena execution status
        :rtype: str
        """
        status = self.client.get_query_execution(QueryExecutionId=execution_id)['QueryExecution']['Status']['State']
        
        if wait:
            while status in ['RUNNING', 'QUEUED']:
                time.sleep(1)
                status = self.client.get_query_execution(QueryExecutionId=execution_id)['QueryExecution']['Status']['State']
        
        return status

    
    def get_query_statistics(self, execution_id):
        """
        Get Athena query statistics assuming a 

        :param str execution_id: The Athena execution ID
        :return: The Athena execution status
        :rtype: str
        """
        status = self.client.get_query_execution(QueryExecutionId=execution_id)['QueryExecution']['Status']['State']
        
        while status in ['RUNNING', 'QUEUED']:
            time.sleep(1)
            status = self.client.get_query_execution(QueryExecutionId=execution_id)['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED']:
            return self.client.get_query_execution(QueryExecutionId=execution_id)['QueryExecution']['Statistics']
        
        return None
    
            
    def get_query_status_message(self, execution_id):
        """
        Get Athena query status message for change of state

        :param str execution_id: The Athena execution ID
        :return: The Athena status message
        :rtype: str
        """
        return self.client.get_query_execution(QueryExecutionId=execution_id)['QueryExecution']['Status']['StateChangeReason']

    
    def get_query_result_s3_uri(self, execution_id):
        """
        Get Athena query output S3 URI.

        :param str execution_id: The Athena execution ID
        :return: The Athena execution output S3 URI
        :rtype: str
        """
        if self.get_query_status(execution_id) == 'SUCCEEDED':
            return self.client.get_query_execution(QueryExecutionId=execution_id)['QueryExecution']['ResultConfiguration']['OutputLocation']
        else:
            return None

        
    def get_query_result_df(self, execution_id, wait=False):
        """
        Get Athena query output in a Pandas dataframe

        :param str execution_id: The Athena execution ID
        :return: The Athena execution output Pandas dataframe
        :rtype: pandas.DataFrame
        """
        if self.get_query_status(execution_id) == 'SUCCEEDED':
            df = pd.read_csv(self.get_query_result_s3_uri(execution_id), header=0)
            return df
        
        status = self.get_query_status(execution_id)
        if wait:
            while status in ['RUNNING', 'QUEUED']:
                time.sleep(1)
                status = self.get_query_status(execution_id)
            if status == 'FAILED':
                print("Query failed")
            if status == 'CANCELLED':
                print("Query cancelled")
            if status == 'SUCCEEDED':
                df = pd.read_csv(self.get_query_result_s3_uri(execution_id), header=0)
                return df
            

    def get_tables(self):
        """
        Get Athena tables

        :return: A list of Athena tables
        :rtype: list[str]
        """
        table_metadata = self.client.list_table_metadata(CatalogName='AwsDataCatalog', DatabaseName=self.athena_database)
        return list(map(lambda x: x['Name'], filter(lambda x: x['TableType'] == 'EXTERNAL_TABLE', table_metadata['TableMetadataList'])))
    

    def get_table_views(self):
        """
        Get Athena table views

        :return: A list of Athena table views
        :rtype: list[str]
        """
        table_metadata = self.client.list_table_metadata(CatalogName='AwsDataCatalog', DatabaseName=self.athena_database)
        return list(map(lambda x: x['Name'], filter(lambda x: x['TableType'] == 'VIRTUAL_VIEW', table_metadata['TableMetadataList'])))
    
    
    def get_table_columns(self, table):
        """
        Get columns from Athena table or table view
        
        :return: A list of columns
        :rtype: list[str]
        """
        table_metadata = self.client.get_table_metadata(CatalogName='AwsDataCatalog', DatabaseName=self.athena_database, TableName=table)
        return list(map(lambda x: x['Name'], table_metadata['TableMetadata']['Columns']))

 
    def get_table_schema(self, table):
        """
        Get schema from Athena table or table view as tuples
        
        :return: A list of tuples for each column with type
        :rtype: list[tuple]
        """
        table_metadata = self.client.get_table_metadata(CatalogName='AwsDataCatalog', DatabaseName=self.athena_database, TableName=table)
        return list(map(lambda x: (x['Name'],x['Type']), table_metadata['TableMetadata']['Columns']))


    def get_table_partition_columns(self, table):
        """
        Get partition columns from Athena table or table view
        
        :return: A list of partition columns
        :rtype: list[str]
        """
        table_metadata = self.client.get_table_metadata(CatalogName='AwsDataCatalog', DatabaseName=self.athena_database, TableName=table)
        return list(map(lambda x: x['Name'], table_metadata['TableMetadata']['PartitionKeys']))
   

    def get_table_partition_schema(self, table):
        """
        Get partition column schema from Athena table or table view as tuples
        
        :return: A list of tuples for each column with type
        :rtype: list[tuple]
        """
        table_metadata = self.client.get_table_metadata(CatalogName='AwsDataCatalog', DatabaseName=self.athena_database, TableName=table)
        return list(map(lambda x: (x['Name'],x['Type']), table_metadata['TableMetadata']['PartitionKeys']))

    
    def get_table_partitions(self, table):
        """
        Get partitions from Athena table
        
        :return: A list of partitions
        :rtype: list[str]
        """
        query = f'show partitions {table}'
        execution_id = self.run_query(query)
        status = self.get_query_status(execution_id)
        while status not in ['SUCCEEDED', 'FAILED']:
            time.sleep(1)
            status = self.get_query_status(execution_id)
        if status == 'FAILED':
            print("Query failed")
        if status == 'SUCCEEDED':
            s3_uri = self.get_query_result_s3_uri(execution_id)

            s3_bucket,s3_object = s3_uri.split('s3://')[1].split('/', maxsplit=1)
            s3 = self.session.client('s3')
            partitions = s3.get_object(Bucket=s3_bucket, Key=s3_object)['Body'].read()
            return partitions.decode("utf-8").splitlines()
