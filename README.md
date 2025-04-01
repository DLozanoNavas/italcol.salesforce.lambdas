# CDK-Based SNS to Lambda Processing Pipeline  

This project implements an AWS CDK-based infrastructure where an AWS Lambda function processes messages received from an SNS topic. The messages originate from database views and are later sent to an external service for consumption.  

## Architecture  

1. **SNS Topic** - Receives messages from database views.  
2. **Lambda Function** - Processes the SNS messages.  
3. **External Service** - Receives the processed data for later consumption.  

## Setup & Deployment  

### Prerequisites  
- AWS CLI installed and configured  
- Node.js installed  
- AWS CDK installed (`npm install -g aws-cdk`)

### Usage
- Lambda function is triggered.
- The Lambda function processes messages incoming from the SNS.
- The processed data is sent to an external service for later use.
