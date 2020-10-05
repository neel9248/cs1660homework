import boto3
import csv

s3=boto3.resource('s3',aws_access_key_id='',aws_secret_access_key='')

#create bucket
s3.create_bucket(Bucket='neeldatatable',CreateBucketConfiguration={'LocationConstraint':'us-west-2'})

#storing object in bucket
s3.Object('neeldatatable','adorable-puppy.jpg ').put(Body=open('/Users/neeltrivedi/code/cs1660/homework2/adorable-puppy.jpg','rb'))


dyndb = boto3.resource('dynamodb',region_name='us-west-2')

# # The first time that we define a table, we use

table=dyndb.create_table(
    TableName='DataTable',
    KeySchema=[
            {'AttributeName':'PartitionKey','KeyType':'HASH'},
            {'AttributeName':'RowKey','KeyType':'RANGE'}
    ] ,
    AttributeDefinitions=[
        { 'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
        { 'AttributeName': 'RowKey',      'AttributeType': 'S'}
        # { 'AttributeName': 'date',        'AttributeType': 'S'},
        # { 'AttributeName': 'description', 'AttributeType': 'S'},
        # { 'AttributeName': 'url',         'AttributeType': 'S'}
    ],
    ProvisionedThroughput= {
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5 
    }
)

# Wait for the table to be created

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

table.put_item(
    Item={
        'PartitionKey': 'S',
        'RowKey': 'S',
        'description': 'S',
        'url': 'S'
    }
)

#If the table has been previously defined thenuse

table = dyndb.Table("DataTable")

urlbase = "https://s3-us-west-2.amazonaws.com/neeldatatable/"
with open('/Users/neeltrivedi/code/cs1660/homework2/experiments.csv', 'rt') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:

        #url=urlbase+item[3]
        metadata_item={'PartitionKey': item[0], 'RowKey':item[1],'description':item[2],'url': item[3]}
        table.put_item(Item=metadata_item)

response = table.get_item( 
    Key={
        'PartitionKey': 'experiment1',
        'RowKey': 'record1' 
    }
)
item = response['Item']
print(item)

response = table.get_item( 
    Key={
        'PartitionKey': 'experiment2',
        'RowKey': 'record2' 
    }
)
item = response['Item']
print(item)




