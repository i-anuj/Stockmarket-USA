# Design Document

## Overview

This design document outlines the architecture and implementation approach for deploying the Stock Market Analytics Dashboard using AWS DevOps services with Infrastructure as Code (Terraform). The solution implements a fully automated CI/CD pipeline that pulls code from GitHub, builds Docker containers, and deploys to ECS Fargate with zero-downtime deployments.

## Architecture

### High-Level Architecture Diagram

```
┌─────────────┐
│   GitHub    │
│ Repository  │
└──────┬──────┘
       │ (webhook/polling)
       ▼
┌─────────────────────────────────────────────────────────┐
│              AWS CodePipeline                            │
│  ┌─────────┐    ┌──────────┐    ┌──────────────┐      │
│  │ Source  │───▶│  Build   │───▶│    Deploy    │      │
│  │ (GitHub)│    │(CodeBuild)│    │(CodeDeploy)  │      │
│  └─────────┘    └──────────┘    └──────────────┘      │
└─────────────────────────────────────────────────────────┘
                        │                    │
                        ▼                    ▼
                  ┌──────────┐        ┌──────────────┐
                  │   ECR    │        │  ECS Cluster │
                  │ Registry │        │  (Fargate)   │
                  └──────────┘        └──────┬───────┘
                                             │
                                             ▼
                                    ┌─────────────────┐
                                    │ Application     │
                                    │ Load Balancer   │
                                    └────────┬────────┘
                                             │
                                             ▼
                                        Internet Users
```

### Network Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                          VPC                                 │
│  ┌────────────────────┐         ┌────────────────────┐     │
│  │   Public Subnet    │         │   Public Subnet    │     │
│  │       AZ-1         │         │       AZ-2         │     │
│  │  ┌──────────────┐  │         │  ┌──────────────┐  │     │
│  │  │     ALB      │  │         │  │     ALB      │  │     │
│  │  └──────────────┘  │         │  └──────────────┘  │     │
│  │  ┌──────────────┐  │         │  ┌──────────────┐  │     │
│  │  │ NAT Gateway  │  │         │  │ NAT Gateway  │  │     │
│  │  └──────────────┘  │         │  └──────────────┘  │     │
│  └────────────────────┘         └────────────────────┘     │
│  ┌────────────────────┐         ┌────────────────────┐     │
│  │  Private Subnet    │         │  Private Subnet    │     │
│  │       AZ-1         │         │       AZ-2         │     │
│  │  ┌──────────────┐  │         │  ┌──────────────┐  │     │
│  │  │  ECS Task    │  │         │  │  ECS Task    │  │     │
│  │  │ (Container)  │  │         │  │ (Container)  │  │     │
│  │  └──────────────┘  │         │  └──────────────┘  │     │
│  └────────────────────┘         └────────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### 1. GitHub Repository Integration

**Purpose**: Source code repository and CI/CD trigger

**Configuration**:
- Repository: Existing stock-market-dashboard repository
- Branch: `main` (production deployments)
- Authentication: GitHub OAuth token or Personal Access Token stored in AWS Secrets Manager
- Webhook: CodePipeline polls or uses CloudWatch Events for change detection

**Interface**:
- Input: Git commits to main branch
- Output: Source code artifact to S3

### 2. AWS CodePipeline

**Purpose**: Orchestrates the CI/CD workflow

**Stages**:

1. **Source Stage**
   - Action: GitHub source action
   - Output: Source code artifact
   - Configuration: Repository name, branch, OAuth token

2. **Build Stage**
   - Action: CodeBuild project
   - Input: Source code artifact
   - Output: Image definitions JSON
   - Configuration: buildspec.yml location

3. **Deploy Stage**
   - Action: ECS deployment
   - Input: Image definitions from build
   - Configuration: ECS cluster name, service name, task definition

**Artifact Storage**: S3 bucket with versioning enabled

**Notifications**: SNS topic for pipeline state changes

### 3. AWS CodeBuild

**Purpose**: Build Docker images and run tests

**Build Environment**:
- Image: `aws/codebuild/standard:7.0` (includes Docker)
- Compute: `BUILD_GENERAL1_SMALL` (3 GB memory, 2 vCPUs)
- Privileged mode: Enabled (required for Docker builds)

**Build Phases** (defined in buildspec.yml):

```yaml
version: 0.2

phases:
  pre_build:
    commands:
      - echo Logging in to Amazon ECR...
      - aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REGISTRY
      - COMMIT_HASH=$(echo $CODEBUILD_RESOLVED_SOURCE_VERSION | cut -c 1-7)
      - IMAGE_TAG=${COMMIT_HASH:=latest}
  
  build:
    commands:
      - echo Build started on `date`
      - echo Installing dependencies...
      - pip install -r requirements.txt
      - echo Running tests...
      - pytest tests/ --verbose
      - echo Building Docker image...
      - docker build -t $ECR_REPOSITORY:latest .
      - docker tag $ECR_REPOSITORY:latest $ECR_REGISTRY/$ECR_REPOSITORY:$IMAGE_TAG
      - docker tag $ECR_REPOSITORY:latest $ECR_REGISTRY/$ECR_REPOSITORY:latest
  
  post_build:
    commands:
      - echo Build completed on `date`
      - echo Pushing Docker images...
      - docker push $ECR_REGISTRY/$ECR_REPOSITORY:$IMAGE_TAG
      - docker push $ECR_REGISTRY/$ECR_REPOSITORY:latest
      - echo Writing image definitions file...
      - printf '[{"name":"stock-dashboard","imageUri":"%s"}]' $ECR_REGISTRY/$ECR_REPOSITORY:$IMAGE_TAG > imagedefinitions.json

artifacts:
  files:
    - imagedefinitions.json
```

**Environment Variables**:
- `AWS_REGION`: AWS region
- `ECR_REGISTRY`: ECR registry URL
- `ECR_REPOSITORY`: ECR repository name

### 4. Amazon ECR

**Purpose**: Store Docker images

**Configuration**:
- Repository name: `stock-market-dashboard`
- Image scanning: Enabled on push
- Encryption: AES-256
- Lifecycle policy:
  ```json
  {
    "rules": [
      {
        "rulePriority": 1,
        "description": "Keep last 10 images",
        "selection": {
          "tagStatus": "any",
          "countType": "imageCountMoreThan",
          "countNumber": 10
        },
        "action": {
          "type": "expire"
        }
      }
    ]
  }
  ```

### 5. Amazon ECS (Fargate)

**Purpose**: Run containerized application

**Cluster Configuration**:
- Name: `stock-dashboard-cluster`
- Capacity providers: `FARGATE`, `FARGATE_SPOT`
- Container Insights: Enabled

**Task Definition**:
```json
{
  "family": "stock-dashboard-task",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "arn:aws:iam::ACCOUNT:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::ACCOUNT:role/ecsTaskRole",
  "containerDefinitions": [
    {
      "name": "stock-dashboard",
      "image": "ECR_IMAGE_URI",
      "portMappings": [
        {
          "containerPort": 8080,
          "protocol": "tcp"
        }
      ],
      "environment": [],
      "secrets": [
        {
          "name": "ALPHA_VANTAGE_API_KEY",
          "valueFrom": "arn:aws:secretsmanager:REGION:ACCOUNT:secret:stock-dashboard/api-key"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/stock-dashboard",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
      },
      "healthCheck": {
        "command": ["CMD-SHELL", "curl -f http://localhost:8080/ || exit 1"],
        "interval": 30,
        "timeout": 5,
        "retries": 3,
        "startPeriod": 60
      }
    }
  ]
}
```

**Service Configuration**:
- Name: `stock-dashboard-service`
- Desired count: 2
- Launch type: Fargate
- Deployment configuration:
  - Minimum healthy percent: 100
  - Maximum percent: 200
  - Deployment circuit breaker: Enabled with rollback
- Load balancer: Application Load Balancer
- Health check grace period: 60 seconds

### 6. Application Load Balancer

**Purpose**: Distribute traffic and perform health checks

**Configuration**:
- Scheme: Internet-facing
- IP address type: IPv4
- Listeners:
  - Port 80 (HTTP) → Forward to target group
- Target group:
  - Protocol: HTTP
  - Port: 8080
  - Target type: IP
  - Health check path: `/`
  - Health check interval: 30 seconds
  - Healthy threshold: 2
  - Unhealthy threshold: 3
  - Timeout: 5 seconds
  - Deregistration delay: 30 seconds

**Security Group**:
- Inbound: Port 80 from 0.0.0.0/0
- Outbound: All traffic

### 7. VPC and Networking

**VPC Configuration**:
- CIDR: `10.0.0.0/16`
- DNS hostnames: Enabled
- DNS resolution: Enabled

**Subnets**:
- Public Subnet AZ1: `10.0.1.0/24`
- Public Subnet AZ2: `10.0.2.0/24`
- Private Subnet AZ1: `10.0.11.0/24`
- Private Subnet AZ2: `10.0.12.0/24`

**Internet Gateway**: Attached to VPC

**NAT Gateways**: One per availability zone in public subnets

**Route Tables**:
- Public: 0.0.0.0/0 → Internet Gateway
- Private: 0.0.0.0/0 → NAT Gateway

### 8. AWS Secrets Manager

**Purpose**: Store sensitive configuration

**Secrets**:
1. `stock-dashboard/api-key`: Alpha Vantage API key
2. `stock-dashboard/github-token`: GitHub OAuth token for CodePipeline

**Configuration**:
- Encryption: AWS managed key
- Rotation: Disabled (manual rotation)

## Data Models

### Terraform State

**Backend Configuration**:
```hcl
terraform {
  backend "s3" {
    bucket         = "stock-dashboard-terraform-state"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}
```

### Terraform Module Structure

```
terraform/
├── main.tf                 # Root module
├── variables.tf            # Input variables
├── outputs.tf              # Output values
├── terraform.tfvars.example # Example variables
├── backend.tf              # Backend configuration
├── modules/
│   ├── vpc/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── ecr/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── ecs/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── alb/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── codepipeline/
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
└── environments/
    ├── dev.tfvars
    ├── staging.tfvars
    └── prod.tfvars
```

### Key Terraform Variables

```hcl
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "github_repo" {
  description = "GitHub repository name"
  type        = string
}

variable "github_branch" {
  description = "GitHub branch to deploy"
  type        = string
  default     = "main"
}

variable "ecs_task_cpu" {
  description = "ECS task CPU units"
  type        = string
  default     = "512"
}

variable "ecs_task_memory" {
  description = "ECS task memory in MB"
  type        = string
  default     = "1024"
}

variable "ecs_desired_count" {
  description = "Desired number of ECS tasks"
  type        = number
  default     = 2
}

variable "alpha_vantage_api_key" {
  description = "Alpha Vantage API key"
  type        = string
  sensitive   = true
}
```

## Error Handling

### Build Failures

**Scenario**: Tests fail during CodeBuild

**Handling**:
1. CodeBuild marks build as FAILED
2. Pipeline stops at Build stage
3. SNS notification sent to operations team
4. CloudWatch Logs retain build output for debugging
5. Previous deployment remains active (no impact to production)

**Recovery**: Developer fixes tests, commits to GitHub, pipeline retries

### Deployment Failures

**Scenario**: New ECS tasks fail health checks

**Handling**:
1. ECS deployment circuit breaker detects failure
2. Automatic rollback to previous task definition
3. CloudWatch alarm triggers for unhealthy targets
4. SNS notification sent
5. Application remains available on previous version

**Recovery**: Developer investigates logs, fixes issue, redeploys

### Infrastructure Failures

**Scenario**: Terraform apply fails

**Handling**:
1. Terraform state remains consistent (atomic operations)
2. Error message indicates failed resource
3. State lock prevents concurrent modifications
4. Previous infrastructure remains intact

**Recovery**: Fix Terraform configuration, retry apply

### API Rate Limiting

**Scenario**: Alpha Vantage API rate limit exceeded

**Handling**:
1. Application logs error to CloudWatch
2. Streamlit displays user-friendly error message
3. CloudWatch alarm triggers if error rate exceeds threshold

**Recovery**: Application automatically retries with exponential backoff (existing behavior)

## Testing Strategy

### Infrastructure Testing

**Terraform Validation**:
```bash
terraform fmt -check
terraform validate
terraform plan
```

**Tools**:
- `terraform-docs`: Generate documentation
- `tflint`: Lint Terraform code
- `checkov`: Security scanning

### Build Testing

**Local Testing**:
```bash
# Test Docker build
docker build -t stock-dashboard:test .

# Test container locally
docker run -p 8080:8080 -e ALPHA_VANTAGE_API_KEY=test stock-dashboard:test

# Run unit tests
pytest tests/
```

**CodeBuild Testing**:
- Use CodeBuild local agent for testing buildspec.yml
- Validate build phases execute correctly
- Verify artifacts are produced

### Deployment Testing

**Pre-Production Validation**:
1. Deploy to dev environment first
2. Run smoke tests against dev ALB endpoint
3. Validate application functionality
4. Promote to staging
5. Run full integration tests
6. Promote to production

**Smoke Tests**:
```bash
# Health check
curl -f http://ALB_DNS/ || exit 1

# Verify API key is loaded
curl http://ALB_DNS/ | grep "Stock Market Analytics Dashboard"
```

### Security Testing

**Vulnerability Scanning**:
- ECR image scanning on push
- Trivy scanning in CI/CD pipeline
- AWS Security Hub for infrastructure

**IAM Policy Validation**:
- Use IAM Access Analyzer
- Validate least privilege principles
- Review CloudTrail logs for unauthorized access

## Deployment Strategy

### Initial Deployment

**Phase 1: Infrastructure Setup**
1. Create S3 bucket and DynamoDB table for Terraform state
2. Store secrets in AWS Secrets Manager
3. Run Terraform to provision infrastructure:
   ```bash
   cd terraform
   terraform init
   terraform plan -var-file=environments/prod.tfvars
   terraform apply -var-file=environments/prod.tfvars
   ```

**Phase 2: Initial Application Deployment**
1. Manually trigger CodePipeline
2. Monitor build and deployment in AWS Console
3. Verify application is accessible via ALB DNS
4. Configure DNS (if applicable) to point to ALB

**Phase 3: Validation**
1. Test application functionality
2. Verify logs in CloudWatch
3. Check CloudWatch metrics and alarms
4. Validate auto-scaling behavior

### Continuous Deployment

**Workflow**:
1. Developer commits code to GitHub main branch
2. CodePipeline automatically triggers
3. CodeBuild runs tests and builds Docker image
4. Image pushed to ECR
5. ECS service updated with new task definition
6. Rolling deployment with health checks
7. Automatic rollback if health checks fail

### Blue-Green Deployment (Optional Enhancement)

For zero-downtime deployments with instant rollback:
1. Use CodeDeploy with ECS blue/green deployment
2. Create new task set with updated image
3. Shift traffic gradually (10%, 50%, 100%)
4. Monitor metrics during traffic shift
5. Automatic rollback if metrics degrade

### Rollback Procedures

**Manual Rollback**:
```bash
# Revert to previous task definition
aws ecs update-service \
  --cluster stock-dashboard-cluster \
  --service stock-dashboard-service \
  --task-definition stock-dashboard-task:PREVIOUS_REVISION
```

**Automated Rollback**:
- ECS deployment circuit breaker handles automatically
- Triggered by failed health checks or deployment timeout

## Monitoring and Observability

### CloudWatch Metrics

**ECS Metrics**:
- CPUUtilization
- MemoryUtilization
- RunningTaskCount
- DesiredTaskCount

**ALB Metrics**:
- TargetResponseTime
- HealthyHostCount
- UnHealthyHostCount
- RequestCount
- HTTPCode_Target_4XX_Count
- HTTPCode_Target_5XX_Count

**CodePipeline Metrics**:
- PipelineExecutionSuccess
- PipelineExecutionFailure
- ActionExecutionDuration

### CloudWatch Alarms

```hcl
# High CPU utilization
resource "aws_cloudwatch_metric_alarm" "ecs_cpu_high" {
  alarm_name          = "stock-dashboard-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ECS"
  period              = "300"
  statistic           = "Average"
  threshold           = "80"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# Unhealthy targets
resource "aws_cloudwatch_metric_alarm" "unhealthy_targets" {
  alarm_name          = "stock-dashboard-unhealthy-targets"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "UnHealthyHostCount"
  namespace           = "AWS/ApplicationELB"
  period              = "60"
  statistic           = "Average"
  threshold           = "0"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
```

### Logging

**Application Logs**:
- Destination: CloudWatch Logs
- Log group: `/ecs/stock-dashboard`
- Retention: 7 days
- Format: JSON structured logs

**Build Logs**:
- Destination: CloudWatch Logs
- Log group: `/aws/codebuild/stock-dashboard`
- Retention: 30 days

**ALB Access Logs**:
- Destination: S3 bucket
- Prefix: `alb-logs/`
- Retention: 90 days

## Security Considerations

### IAM Roles and Policies

**CodeBuild Service Role**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/codebuild/*"
    }
  ]
}
```

**ECS Task Execution Role**:
- Managed policy: `AmazonECSTaskExecutionRolePolicy`
- Additional: Secrets Manager read access

**ECS Task Role**:
- Minimal permissions (none required for this application)
- Add only if application needs AWS service access

### Network Security

**Security Groups**:
1. ALB Security Group:
   - Inbound: 0.0.0.0/0:80
   - Outbound: ECS security group:8080

2. ECS Security Group:
   - Inbound: ALB security group:8080
   - Outbound: 0.0.0.0/0:443 (for API calls)

**Network ACLs**: Use default (allow all)

### Secrets Management

- Store all secrets in AWS Secrets Manager
- Never commit secrets to Git
- Use IAM policies to restrict secret access
- Enable secret rotation where possible
- Audit secret access via CloudTrail

## Cost Optimization

### Resource Sizing

**Development Environment**:
- ECS tasks: 1 task, 256 CPU, 512 MB memory
- Fargate Spot for cost savings
- Single NAT Gateway

**Production Environment**:
- ECS tasks: 2 tasks, 512 CPU, 1024 MB memory
- Fargate On-Demand for reliability
- NAT Gateway per AZ for high availability

### Cost Estimates (Monthly)

**Production Environment**:
- ECS Fargate (2 tasks): ~$30
- Application Load Balancer: ~$20
- NAT Gateway (2): ~$65
- ECR storage (10 images): ~$1
- CloudWatch Logs: ~$5
- Data transfer: ~$10
- **Total**: ~$131/month

**Development Environment**:
- ECS Fargate Spot (1 task): ~$8
- Application Load Balancer: ~$20
- NAT Gateway (1): ~$32
- Other services: ~$5
- **Total**: ~$65/month

### Cost Optimization Strategies

1. Use Fargate Spot for non-production (70% savings)
2. Implement ECR lifecycle policies
3. Set CloudWatch Logs retention policies
4. Use single NAT Gateway for dev/staging
5. Schedule ECS tasks to scale down during off-hours
6. Enable S3 lifecycle policies for logs and artifacts

## Performance Considerations

### Application Performance

- **Target Response Time**: < 2 seconds for page load
- **Concurrent Users**: Support 100+ concurrent users
- **API Rate Limiting**: Implement caching to reduce API calls

### Infrastructure Performance

- **Deployment Time**: < 10 minutes for full pipeline
- **Scaling**: Auto-scale based on CPU/memory metrics
- **Health Check**: 30-second interval, 5-second timeout

### Optimization Techniques

1. Enable ALB connection draining (30 seconds)
2. Use ECS task placement strategies for even distribution
3. Implement CloudFront CDN for static assets (future enhancement)
4. Enable ALB access logs for performance analysis

## Disaster Recovery

### Backup Strategy

**Infrastructure**:
- Terraform state backed up in S3 with versioning
- Infrastructure can be recreated from Terraform code

**Application**:
- Docker images stored in ECR with lifecycle retention
- Source code in GitHub with full history

**Data**:
- No persistent data storage (stateless application)
- Configuration in Secrets Manager with automatic backups

### Recovery Procedures

**Complete Region Failure**:
1. Update Terraform variables for new region
2. Run `terraform apply` in new region
3. Update DNS to point to new ALB
4. RTO: 30 minutes, RPO: 0 (no data loss)

**Service Failure**:
1. ECS automatically restarts failed tasks
2. ALB routes traffic to healthy tasks
3. Auto-scaling replaces unhealthy instances
4. RTO: 2 minutes, RPO: 0

## Documentation Deliverables

### 1. README-DEVOPS.md
- Architecture overview
- Prerequisites
- Deployment instructions
- Troubleshooting guide

### 2. DEPLOYMENT.md
- Step-by-step deployment guide
- Environment setup
- Terraform commands
- Validation steps

### 3. Architecture Diagrams
- CI/CD workflow diagram
- AWS infrastructure diagram
- Network topology diagram

### 4. Runbooks
- Deployment runbook
- Rollback runbook
- Incident response runbook
- Scaling runbook

## Future Enhancements

1. **HTTPS/SSL**: Add ACM certificate and HTTPS listener
2. **Custom Domain**: Route53 hosted zone and DNS records
3. **WAF**: AWS WAF for application protection
4. **Auto-scaling**: Target tracking scaling policies
5. **Multi-region**: Active-active deployment across regions
6. **Blue-Green Deployment**: Zero-downtime deployments with instant rollback
7. **Canary Deployments**: Gradual traffic shifting with automated rollback
8. **Container Insights**: Enhanced monitoring with Container Insights
9. **X-Ray**: Distributed tracing for performance analysis
10. **Secrets Rotation**: Automatic rotation of API keys and credentials
