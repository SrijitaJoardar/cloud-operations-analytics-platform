"""
Application constants for the Cloud Operations Analytics Platform.

This module contains fixed values used throughout the project.
"""
CLOUD_PROVIDERS = [
    "AWS",
]
ACCOUNT_IDS = [
    "123456789012",
    "234567890123",
    "345678901234",
]
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

REGION_TO_AVAILABILITY_ZONES = {
    "ap-south-1": [
        "ap-south-1a",
        "ap-south-1b",
    ],
    "us-east-1": [
        "us-east-1a",
        "us-east-1b",
        "us-east-1c",
    ],
    "us-west-2": [
        "us-west-2a",
        "us-west-2b",
    ],
    "eu-west-1": [
        "eu-west-1a",
        "eu-west-1b",
    ],
    "ap-southeast-1": [
        "ap-southeast-1a",
        "ap-southeast-1b",
    ],
}
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

RUNNING_STATUS = "Running"

STOPPED_STATUS = "Stopped"

STATUS = [
    RUNNING_STATUS,
    STOPPED_STATUS,
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


INSTANCE_TYPE_HOURLY_COST = {
    "t3.micro": 0.010,
    "t3.small": 0.020,
    "t3.medium": 0.040,
    "m5.large": 0.096,
    "m5.xlarge": 0.192,
    "c5.large": 0.085,
}

CPU_USAGE_MIN = 0
CPU_USAGE_MAX = 100

MEMORY_USAGE_MIN = 0
MEMORY_USAGE_MAX = 100

RUNNING_HOURS_MIN = 0
RUNNING_HOURS_MAX = 24

DAILY_COST_MIN = 0
DAILY_COST_MAX = 1000