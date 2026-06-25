"""
Application constants for the Cloud Operations Analytics Platform.

This module contains fixed values used throughout the project.
"""

SERVICES = [
    "EC2",
    "S3",
    "RDS",
    "Lambda",
    "EKS",
    "ECS",
]

REGIONS = [
    "ap-south-1",
    "ap-southeast-1",
    "us-east-1",
    "us-west-2",
    "eu-west-1",
]

INSTANCE_TYPES = [
    "t3.micro",
    "t3.small",
    "t3.medium",
    "m5.large",
    "m5.xlarge",
    "c5.large",
]

ENVIRONMENTS = [
    "Development",
    "QA",
    "UAT",
    "Production",
]

STATUS = [
    "Running",
    "Stopped",
    "Terminated",
]

PROJECT_NAMES = [
    "Customer360",
    "Cloud Migration",
    "Payment Gateway",
    "Recommendation Engine",
    "Inventory System",
    "Analytics Platform",
]

TEAM_NAMES = [
    "Data Engineering",
    "Platform Engineering",
    "DevOps",
    "Backend",
    "AI & ML",
    "QA Automation",
]