#!/usr/bin/env python3
import os

import aws_cdk as cdk

from italcol_salesforce_lambdas.italcol_salesforce_lambdas_stack import ItalcolSalesforceLambdasStack

app = cdk.App()
ItalcolSalesforceLambdasStack(app, "ItalcolSalesforceLambdasStack",

    # Stack deployed with corresponding account and region configured
    env=cdk.Environment(account="123456789012", region="us-east-1"),
    )

app.synth()
