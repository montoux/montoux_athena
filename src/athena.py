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

    
    def get_query_status(self, execution_id):
        """
        Get Athena query status.

        :param str execution_id: The Athena execution ID
        :return: The Athena execution status
        :rtype: str
        """
        result = self.client.get_query_execution(QueryExecutionId=execution_id)
        return result['QueryExecution']['Status']['State']
    
    
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
        :rtype: pd.DataFrame
        """
        if self.get_query_status(execution_id) == 'SUCCEEDED':
            df = pd.read_csv(self.get_query_result_s3_uri(execution_id), header=0)
            return df
        
        status = self.get_query_status(execution_id)
        if wait:
            while status not in ['SUCCEEDED', 'FAILED']:
                time.sleep(1)
                status = self.get_query_status(execution_id)
            if status == 'FAILED':
                pritn("Query failed")
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
