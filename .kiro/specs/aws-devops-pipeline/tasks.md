# Implementation Plan

- [x] 1. Set up Terraform backend and project structure
  - Create S3 bucket and DynamoDB table for Terraform state management
  - Create Terraform directory structure with modules folder
  - Write backend.tf with S3 backend configuration
  - Write main.tf root module file with provider configuration
  - Create variables.tf with all required input variables
  - Create outputs.tf with ALB DNS and other important outputs
  - Create terraform.tfvars.example with example values
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

- [x] 2. Create VPC Terraform module
  - Create modules/vpc directory structure
  - Write modules/vpc/main.tf with VPC, subnets, IGW, NAT gateways, and route tables
  - Write modules/vpc/variables.tf for CIDR blocks and availability zones
  - Write modules/vpc/outputs.tf exposing VPC ID, subnet IDs, and security group IDs
  - Configure 2 public subnets and 2 private subnets across 2 AZs
  - Create Internet Gateway and NAT Gateways for outbound connectivity
  - _Requirements: 5.1, 5.5_

- [x] 3. Create ECR Terraform module
  - Create modules/ecr directory structure
  - Write modules/ecr/main.tf with ECR repository resource
  - Configure image scanning on push for vulnerability detection
  - Implement lifecycle policy to retain only last 10 images
  - Enable encryption at rest with AES-256
  - Write modules/ecr/variables.tf for repository name and lifecycle rules
  - Write modules/ecr/outputs.tf exposing repository URL and ARN
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 10.3_

- [x] 4. Create ALB Terraform module
  - Create modules/alb directory structure
  - Write modules/alb/main.tf with ALB, target group, and listener resources
  - Configure ALB in public subnets with internet-facing scheme
  - Create target group with IP target type for ECS Fargate
  - Configure health check on path "/" with 30-second interval
  - Create security group allowing inbound port 80 from internet
  - Write modules/alb/variables.tf for VPC ID, subnet IDs, and health check parameters
  - Write modules/alb/outputs.tf exposing ALB DNS name, ARN, and target group ARN
  - _Requirements: 5.2, 5.3, 5.4, 5.6_

- [x] 5. Create IAM roles Terraform module
  - Create modules/iam directory structure
  - Write modules/iam/main.tf with all required IAM roles and policies
  - Create ECS task execution role with ECR and Secrets Manager permissions
  - Create ECS task role with minimal permissions
  - Create CodeBuild service role with ECR push and CloudWatch Logs permissions
  - Create CodePipeline service role with CodeBuild and ECS permissions
  - Write modules/iam/variables.tf for resource naming
  - Write modules/iam/outputs.tf exposing all role ARNs
  - _Requirements: 8.1, 8.2, 8.3, 8.4_

- [x] 6. Create ECS Terraform module
  - Create modules/ecs directory structure
  - Write modules/ecs/main.tf with ECS cluster, task definition, and service resources
  - Configure Fargate launch type with 512 CPU and 1024 MB memory
  - Create task definition with container definition for stock-dashboard
  - Configure port mapping for port 8080
  - Set up CloudWatch Logs configuration with 7-day retention
  - Configure secrets injection from AWS Secrets Manager for API key
  - Create ECS service with desired count of 2 tasks
  - Configure ALB integration with target group
  - Enable deployment circuit breaker with rollback
  - Create security group allowing inbound traffic only from ALB
  - Write modules/ecs/variables.tf for cluster name, task resources, and desired count
  - Write modules/ecs/outputs.tf exposing cluster ID and service name
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 5.7, 8.6_

- [x] 7. Create CodeBuild Terraform module
  - Create modules/codebuild directory structure
  - Write modules/codebuild/main.tf with CodeBuild project resource
  - Configure build environment with aws/codebuild/standard:7.0 image
  - Enable privileged mode for Docker builds
  - Set compute type to BUILD_GENERAL1_SMALL
  - Configure environment variables for AWS_REGION, ECR_REGISTRY, ECR_REPOSITORY
  - Set buildspec location to buildspec.yml in repository root
  - Configure CloudWatch Logs with 30-day retention
  - Attach IAM service role for ECR and CloudWatch permissions
  - Write modules/codebuild/variables.tf for project name and environment variables
  - Write modules/codebuild/outputs.tf exposing project name and ARN
  - _Requirements: 2.1, 2.8, 8.1_

- [x] 8. Create CodePipeline Terraform module
  - Create modules/codepipeline directory structure
  - Write modules/codepipeline/main.tf with CodePipeline resource
  - Create S3 bucket for pipeline artifacts with versioning enabled
  - Configure Source stage with GitHub source action using OAuth token from Secrets Manager
  - Configure Build stage with CodeBuild action
  - Configure Deploy stage with ECS deployment action
  - Create SNS topic for pipeline notifications
  - Attach IAM service role with permissions for all stages
  - Write modules/codepipeline/variables.tf for GitHub repo, branch, and service names
  - Write modules/codepipeline/outputs.tf exposing pipeline name and ARN
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_

- [x] 9. Create CloudWatch alarms Terraform module
  - Create modules/monitoring directory structure
  - Write modules/monitoring/main.tf with CloudWatch alarm resources
  - Create alarm for ECS CPU utilization above 80%
  - Create alarm for ECS memory utilization above 80%
  - Create alarm for ALB unhealthy target count greater than 0
  - Create alarm for CodePipeline execution failures
  - Configure all alarms to send notifications to SNS topic
  - Write modules/monitoring/variables.tf for alarm thresholds and SNS topic ARN
  - Write modules/monitoring/outputs.tf exposing alarm ARNs
  - _Requirements: 9.4, 9.5, 9.6_

- [x] 10. Create Secrets Manager resources in Terraform
  - Add AWS Secrets Manager resources to root main.tf
  - Create secret for Alpha Vantage API key
  - Create secret for GitHub OAuth token
  - Configure encryption with AWS managed key
  - Add data source to read existing secrets if they already exist
  - _Requirements: 4.5, 8.5_

- [x] 11. Wire up all Terraform modules in root configuration
  - Update root main.tf to instantiate all modules (VPC, ECR, ALB, IAM, ECS, CodeBuild, CodePipeline, monitoring)
  - Pass outputs from one module as inputs to dependent modules
  - Configure module dependencies in correct order
  - Add tags to all resources with environment and project name
  - Validate all module connections are correct
  - _Requirements: 6.2, 6.7_

- [x] 12. Create environment-specific variable files
  - Create terraform/environments directory
  - Write environments/dev.tfvars with development configuration (1 task, smaller resources)
  - Write environments/staging.tfvars with staging configuration
  - Write environments/prod.tfvars with production configuration (2 tasks, full resources)
  - Document variable differences between environments
  - _Requirements: 10.5, 12.1, 12.2, 12.3, 12.4, 12.5_

- [x] 13. Create buildspec.yml for CodeBuild
  - Create buildspec.yml in repository root
  - Write pre_build phase with ECR login and image tag generation
  - Write build phase with dependency installation, pytest execution, and Docker build
  - Write post_build phase with Docker image push and imagedefinitions.json creation
  - Add inline comments explaining each command
  - Configure artifacts section to output imagedefinitions.json
  - Add error handling to fail build if tests fail
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 11.1_

- [x] 14. Update Dockerfile for production deployment
  - Review existing Dockerfile for production readiness
  - Add health check instruction for container health monitoring
  - Optimize layer caching for faster builds
  - Add non-root user for security best practices
  - Ensure port 8080 is properly exposed
  - Add labels for image metadata
  - _Requirements: 2.5_

- [x] 15. Create GitHub Actions workflow for Terraform validation
  - Create .github/workflows directory
  - Write terraform-validate.yml workflow file
  - Configure workflow to run on pull requests to main branch
  - Add job to run terraform fmt -check
  - Add job to run terraform validate
  - Add job to run terraform plan
  - Configure AWS credentials using GitHub secrets
  - _Requirements: 1.1_

- [ ]* 16. Create deployment documentation
  - Create docs/DEPLOYMENT.md with step-by-step deployment instructions
  - Document prerequisites (AWS CLI, Terraform, GitHub token)
  - Write instructions for creating Terraform backend resources
  - Document how to store secrets in AWS Secrets Manager
  - Write instructions for running Terraform init, plan, and apply
  - Document how to verify deployment and access application
  - Include troubleshooting section for common issues
  - _Requirements: 11.4_

- [ ]* 17. Create DevOps architecture documentation
  - Create docs/README-DEVOPS.md with architecture overview
  - Include CI/CD workflow diagram (ASCII or Mermaid)
  - Include AWS infrastructure diagram
  - Document all AWS services used and their purpose
  - Explain security model and IAM roles
  - Document monitoring and logging setup
  - Include cost breakdown and optimization strategies
  - _Requirements: 11.2, 11.5_

- [ ]* 18. Create Terraform documentation
  - Add README.md to terraform/ directory
  - Document module structure and dependencies
  - List all required variables and their purposes
  - Document outputs and how to use them
  - Include examples of running Terraform commands
  - Document how to manage multiple environments
  - _Requirements: 11.3_

- [ ]* 19. Create runbooks for operations
  - Create docs/runbooks directory
  - Write deployment-runbook.md with deployment procedures
  - Write rollback-runbook.md with rollback procedures
  - Write scaling-runbook.md with manual scaling instructions
  - Write incident-response-runbook.md for handling production issues
  - Include AWS CLI commands for common operations
  - _Requirements: 11.4_

- [-] 20. Create helper scripts for deployment
  - Create scripts/deploy.sh for automated Terraform deployment
  - Create scripts/destroy.sh for tearing down infrastructure
  - Create scripts/validate.sh for running Terraform validation
  - Create scripts/setup-backend.sh for creating S3 and DynamoDB resources
  - Add error handling and validation to all scripts
  - Make all scripts executable with proper shebang
  - _Requirements: 6.4_

- [ ]* 21. Add cost estimation script
  - Create scripts/estimate-costs.sh using AWS Pricing API
  - Calculate estimated monthly costs for each environment
  - Output cost breakdown by service
  - Compare costs across environments
  - _Requirements: 10.1, 10.2, 10.3, 10.4_

- [ ] 22. Update project README with DevOps information
  - Add "Deployment" section to main README.md
  - Link to DEPLOYMENT.md and README-DEVOPS.md
  - Add badges for build status (GitHub Actions)
  - Document how to access deployed application
  - Add section on CI/CD pipeline
  - _Requirements: 11.2_

- [ ] 23. Create .gitignore entries for Terraform
  - Add .terraform/ directory to .gitignore
  - Add *.tfstate and *.tfstate.backup to .gitignore
  - Add .terraform.lock.hcl to .gitignore (or commit for version locking)
  - Add terraform.tfvars to .gitignore (contains sensitive values)
  - Add .terraform.tfvars.backup to .gitignore
  - _Requirements: 6.4_

- [ ] 24. Create initial Terraform state backend resources
  - Write scripts/setup-backend.sh to create S3 bucket for state
  - Configure S3 bucket versioning and encryption
  - Create DynamoDB table for state locking
  - Add bucket policy to prevent accidental deletion
  - Output bucket name and DynamoDB table name for backend configuration
  - _Requirements: 6.3_

- [ ]* 25. Validate complete infrastructure deployment
  - Run Terraform plan to validate all modules
  - Review plan output for correctness
  - Check for any security issues or misconfigurations
  - Verify all required resources are included
  - Validate IAM policies follow least privilege
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_

- [ ]* 26. Create smoke tests for deployed application
  - Create tests/smoke directory
  - Write test_deployment.py to verify ALB health
  - Write test to verify application responds on ALB endpoint
  - Write test to verify application loads without errors
  - Add script to run smoke tests against deployed environment
  - _Requirements: 4.6, 4.7, 4.8_
