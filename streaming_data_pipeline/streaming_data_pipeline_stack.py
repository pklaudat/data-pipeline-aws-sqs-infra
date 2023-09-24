from aws_cdk import (
    aws_sqs as sqs,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_s3 as s3,
    Duration,
    CfnOutput,
    RemovalPolicy,
    Stack
)
from constructs import Construct

class StreamingDataPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an SQS queue for data ingestion
        data_queues = list()
        data_sources = ['employee', 'claims', 'banks']

        for source in data_sources:
            data_queues.append(sqs.Queue(
                self,
                f"dataqueue-{source}",
                visibility_timeout=Duration.seconds(300),  # Adjust as needed
                queue_name=f'{source}-data-queue'
            ))

        data_sources = list()
        for bucket in ['rawtobronze', 'bronzetosilver', 'silvertogold']:
            data_sources.append(
                s3.Bucket(self, bucket,
                    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                    encryption=s3.BucketEncryption.S3_MANAGED,
                    enforce_ssl=True,
                    bucket_name=f"{self.account}{bucket}",
                    versioned=False,
                    removal_policy=RemovalPolicy.DESTROY
                )
            )

        # Define an IAM role for Lambda
        lambda_role = iam.Role(
            self,
            "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="IAM role for Lambda function",
        )

        # Attach necessary policies to the Lambda role (e.g., for SQS access)
        for managed_policy_name in ['AmazonSQSFullAccess', 'AmazonS3FullAccess']:
            lambda_role.add_managed_policy(
                iam.ManagedPolicy.from_aws_managed_policy_name(managed_policy_name)
            )

        # Create a Lambda function to process data from SQS
        lambdas = list()
        lambda_sources = ['ingest-employee', 'ingest-claims', 'ingest-banks']
        for lambda_name in lambda_sources:
            lambdas.append(
                _lambda.Function(
                    self,
                    f"{lambda_name}-lambda",
                    runtime=_lambda.Runtime.PYTHON_3_10,
                    handler="lambda.handler",
                    function_name=f"{lambda_name}-lambda",
                    code=_lambda.Code.from_asset("lambdas/ingest_data"),  # Replace with your Lambda code location
                    role=lambda_role,
                    timeout=Duration.seconds(30),
                    environment={
                        "QUEUE_URL": data_queues[lambda_sources.index(lambda_name)].queue_url,  # Pass the SQS queue URL as an environment variable
                        "BUCKET_NAME": data_sources[0].bucket_name,
                        "S3_KEY": lambda_name.replace("ingest-","")
                    },
                )
            )

        # trigger lambda via sqs message
        for l in lambdas:
            l.add_event_source(event_sources.SqsEventSource(data_queues[lambdas.index(l)]))

        etl_lambdas = list()
        lambas_etl = ['process', 'deliver']
        for lambda_process in lambas_etl:
            etl_lambdas.append(
                _lambda.Function(
                    self,
                    f"{lambda_process}-lambda",
                    runtime=_lambda.Runtime.PYTHON_3_10,
                    handler="lambda.handler",
                    function_name=f"{lambda_process}-lambda",
                    code=_lambda.Code.from_asset("lambdas"),  # Replace with your Lambda code location
                    role=lambda_role,
                    timeout=Duration.seconds(30),
                    environment={
                        "BUCKET_READ": data_sources[lambas_etl.index(lambda_process)].bucket_name,
                        "BUCKET_WRITE": data_sources[lambas_etl.index(lambda_process)+1].bucket_name
                    },
                )
            )

        lambdas.extend(etl_lambdas)

        steps = list()

        task_list = ['ingest-employee-task', 'ingest-claims-task', 'ingest-banks-task', 'process-task', 'deliver-task']
        for task in task_list:
            steps.append(
                sfn_tasks.LambdaInvoke(
                    self, task, lambda_function=lambdas[task_list.index(task)]
                )
            )

        parallel_task = sfn.Parallel(self, 'parallel-ingestion').branch(steps[0]).branch(steps[1]).branch(steps[2])
        parallel_task.next(steps[3])
        steps[3].next(steps[4])

        medallion_state_machine = sfn.StateMachine(
            self,
            "DataIngestionStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(parallel_task),
            state_machine_name="data-pipeline"
        )

        # Output the State Machine ARN for reference
        CfnOutput(
            self,
            "StateMachineArn",
            value=medallion_state_machine.state_machine_arn,
            description="ARN of the Medallion State Machine",
        )



        