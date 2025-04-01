import aws_cdk as core
import aws_cdk.assertions as assertions

from italcol.salesforce.lambdas.italcol.salesforce.lambdas_stack import ItalcolSalesforceLambdasStack

# example tests. To run these tests, uncomment this file along with the example
# resource in italcol.salesforce.lambdas/italcol.salesforce.lambdas_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ItalcolSalesforceLambdasStack(app, "italcol-salesforce-lambdas")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
