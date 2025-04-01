#!/usr/bin/env python3
import os

import aws_cdk as cdk

from italcol_salesforce_lambdas.italcol_salesforce_lambdas_stack import ItalcolSalesforceLambdasStack


account = os.getenv("CDK_DEFAULT_ACCOUNT", os.getenv("AWS_ACCOUNT_ID"))
region = os.getenv("CDK_DEFAULT_REGION", os.getenv("AWS_REGION"))

app = cdk.App()
ItalcolSalesforceLambdasStack(app, "ItalcolSalesforceLambdasStack",

    # Stack deployed with corresponding account and region configured
    env=cdk.Environment(account=account, region=region),
    )

app.synth()
