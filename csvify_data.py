# Unify the results into a single csv files containing all resources
import csv, json, os


# def process_(operation):

"""
Process each output file to create consistent records
"""
class OutputFileProcessor:

    def process_file(self, file):

        f = open(file, "r")
        data = json.loads(f.read())
        f.close()

        method_name = data['service'] + '_' + data['operation'].lower()

        records = []

        # Check if has a method to process this file
        if hasattr(self, method_name):
            method_to_call = getattr(self, method_name)
            if callable(method_to_call):
                records += method_to_call(data)

        return records

    def __create_record(self, region, service, resource_name, arn=None, extra=None):
        return {
            "region": region,
            "service": service,
            "resource_name": resource_name,
            "arn": arn,
            "extra": extra
        }

    """
    Print sample data from file
    """
    def __debug_data(self, data):
        for rootkey in data:
            if rootkey != 'response':
                print(rootkey+': '+str(data[rootkey]))
        for key in data['response']:
            if key == 'ResponseMetadata':
                continue
            if type(data['response'][key]) is list:
                print('----')
                print(key)
                print(json.dumps(data['response'][key][0:2],indent=2))
            else:
                print('----')
                print(key)
                print(data['response'][key])

    """
    Helper to generate string from object and set of keys
    """
    def __generate_comment(self, data, keys):
        msgs = []
        for key in keys:
            msgs.append(key + ": "+str(data[key]))
        return ", ".join(msgs)

    def iam_listsamlproviders(self, data):
        records = []
        for item in data['response']['SAMLProviderList']:
            records.append(self.__create_record(data['region'], data['service'], 'SAML-Providers', item['Arn'] ))
        return records

    def ec2_describetags(self, data):
        records = []
        for item in data['response']['Tags']:

            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Ec2 Tag',
                None,
                self.__generate_comment(item,['Value','Key'])
            ))
        return records

    def cloudformation_listexports(self, data):
        records = []
        for item in data['response']['Exports']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'CF Exports',
                None,
                self.__generate_comment(item, ['Value', 'Name'])
            ))
        return records

    def ec2_describesubnets(self, data):
        records = []
        for item in data['response']['Subnets']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Subnets',
                item['SubnetArn']
            ))
        return records

    def logs_describeloggroups(self, data):
        records = []
        for item in data['response']['logGroups']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Log Group',
                item['arn'],
                self.__generate_comment(item, ['logGroupName'])
            ))
        return records

    def cloudwatch_listmetrics(self, data):
        records = []
        for item in data['response']['Metrics']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Metric',
                None,
                self.__generate_comment(item, ['MetricName','Namespace'])
            ))
        return records

    def ec2_describeinstances(self, data):
        records = []
        for group in data['response']['Reservations']:
            for item in group['Instances']:
                records.append(self.__create_record(
                    data['region'],
                    data['service'],
                    'EC2 Instances',
                    None,
                    self.__generate_comment(item, ['InstanceId','VpcId','KeyName'])
                ))
        return records

    def cloudformation_liststacks(self, data):
        records = []
        for item in data['response']['StackSummaries']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'CF Stack',
                item['StackId'],
                self.__generate_comment(item, ['StackName'])
            ))
        return records

    def ec2_describenetworkinterfaces(self, data):
        records = []
        for item in data['response']['NetworkInterfaces']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Network Interface',
                None,
                self.__generate_comment(item, ['VpcId','InterfaceType'])
            ))
        return records

    def ec2_describesecuritygroups(self, data):
        records = []
        for item in data['response']['SecurityGroups']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'SecurityGroup',
                None,
                self.__generate_comment(item, ['Description','GroupName'])
            ))
        return records

    def ec2_describeroutetables(self, data):
        records = []
        for item in data['response']['RouteTables']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'RouteTable',
                None,
                self.__generate_comment(item, ['VpcId'])
            ))
        return records

    def lambda_listfunctions(self, data):

        records = []
        for item in data['response']['Functions']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Lambda Function',
                item['FunctionArn'],
                self.__generate_comment(item, ['FunctionName','MemorySize','Timeout'])
            ))
        return records

    def iam_listroles(self, data):
        records = []
        for item in data['response']['Roles']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Role',
                item['Arn'],
                self.__generate_comment(item, ['RoleName'])
            ))
        return records

    def s3_listbuckets(self, data):

        records = []
        for item in data['response']['Buckets']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Bucket',
                None,
                self.__generate_comment(item, ['Name'])
            ))
        return records

    def ec2_describekeypairs(self, data):

        records = []
        for item in data['response']['KeyPairs']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'KeyPair',
                None,
                self.__generate_comment(item, ['KeyName'])
            ))
        return records

    def route53_listhostedzonesbyname(self, data):

        records = []
        for item in data['response']['HostedZones']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'HostedZone',
                None,
                self.__generate_comment(item, ['Name'])
            ))
        return records

    def iam_listpolicies(self, data):

        records = []
        for item in data['response']['Policies']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Policy',
                item['Arn'],
                self.__generate_comment(item, ['PolicyName'])
            ))
        return records

    def apigateway_getdomainnames(self, data):

        records = []
        for item in data['response']['items']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Domain Name',
                None,
                self.__generate_comment(item, ['domainName'])
            ))
        return records

    def ec2_describeinternetgateways(self, data):

        records = []
        for item in data['response']['InternetGateways']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'InternetGateway',
                None,
                self.__generate_comment(item, ['InternetGatewayId'])
            ))
        return records

    def dynamodb_listtables(self, data):
        records = []
        for item in data['response']['TableNames']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Dynamo DB Table',
                None,
                "Name: "+item
            ))
        return records

    def iam_listusers(self, data):

        records = []
        for item in data['response']['Users']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'User',
                item['Arn'],
                None
            ))
        return records

    def ec2_describevpcs(self, data):

        records = []
        for item in data['response']['Vpcs']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'VPC',
                None,
                self.__generate_comment(item, ['VpcId'])
            ))
        return records

    def ec2_describenatgateways(self, data):

        records = []
        for item in data['response']['NatGateways']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'NatGateway',
                None,
                self.__generate_comment(item, ['VpcId'])
            ))
        return records

    def sqs_listqueues(self, data):

        records = []
        for item in data['response']['QueueUrls']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'SQS Queue',
                None,
                item
            ))
        return records

    def apigatewayv2_getdomainnames(self, data):

        records = []
        for item in data['response']['Items']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'API Gw Domain',
                None,
                self.__generate_comment(item, ['DomainName'])
            ))
        return records

    def sns_listsubscriptions(self, data):

        records = []
        for item in data['response']['Subscriptions']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Subscription',
                item['SubscriptionArn'],
                self.__generate_comment(item, ['Protocol','TopicArn','Endpoint'])
            ))
        return records

    def sns_listtopics(self, data):

        records = []
        for item in data['response']['Topics']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Topic',
                item['TopicArn']
            ))
        return records

    def lambda_listeventsourcemappings(self, data):

        records = []
        for item in data['response']['EventSourceMappings']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'EventSourceMapping',
                None,
                self.__generate_comment(item, ['FunctionArn','EventSourceArn','BatchSize'])
            ))
        return records

    def ses_listidentities(self, data):

        records = []
        for item in data['response']['Identities']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'Identity',
                None,
                item
            ))
        return records

    def route53_listhostedzones(self, data):

        records = []
        for item in data['response']['HostedZones']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'HostedZone',
                None,
                self.__generate_comment(item, ['Name'])
            ))
        return records

    def apigateway_getrestapis(self, data):

        records = []
        for item in data['response']['items']:
            records.append(self.__create_record(
                data['region'],
                data['service'],
                'NatGateways',
                None,
                self.__generate_comment(item, ['name'])
            ))
        return records


def csvify_data(path='./data/'):
    _, _, filenames = next(os.walk(path))

    processor = OutputFileProcessor()
    records = []

    for file in filenames:
        file = path + file
        records += processor.process_file(file)

    print("{} major AWS resource found.".format(len(records)))

    keys = records[0].keys()
    with open('data/aws-general-output.csv', 'w')  as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(records)
    print("Output file written to `data/aws-general-output.csv`")

csvify_data()
