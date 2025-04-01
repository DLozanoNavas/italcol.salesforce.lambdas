from aws_cdk import (
    Stack,
    aws_lambda as lambda_
)
from constructs import Construct

class ItalcolSalesforceLambdasStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Lambda function is setup
        lambda_function = lambda_.Function(
            self, "salesforce-terceros-casabaysanta",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("lambda")
        )

