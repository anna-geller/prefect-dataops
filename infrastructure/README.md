# Cloud Formation templates used to deploy infrastructure on AWS

You may replace that with Terraform if that's your preference. 
Managing Kubernetes clusters on AWS EKS involves creation of many resources. 
Doing this in Terraform requires expert-level AWS knowledge. 
We deliberately rely on ``eksctl`` here to simplify the process for you while still maintaining all benefits of Infrastructure as Code due to YAML-based declarative cluster definition and CloudFormation stack that gets deployed under the hood.

