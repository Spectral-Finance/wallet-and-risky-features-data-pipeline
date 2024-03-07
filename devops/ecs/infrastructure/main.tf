provider "aws" {
  region = "us-east-2"

  default_tags {
    tags = {
      environment = local.env
      project     = local.project
      team        = local.team
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

# Create log group
resource "aws_cloudwatch_log_group" "this" {
  name = "/ecs/log-group-${local.project}-${local.env}"
}

# Create ECR repository
resource "aws_ecr_repository" "this" {
  name = "data-lakehouse/${local.project}-${local.env}"
}

resource "null_resource" "build_and_docker_image" {

  provisioner "local-exec" {
    command = "/bin/sh build_and_push_ecr_image.sh ${local.project} ${local.env}"
  }
}

# Create ECS cluster
resource "aws_ecs_cluster" "this" {
  name = "${local.project}-${local.env}-fargate-cluster"
}

# Define ECS capacity provider
resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    base              = 0
    weight            = 100
  }
}

# Create ECS task definition
resource "aws_ecs_task_definition" "this" {
  family                   = "${local.project}-${local.env}-fargate"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 4096
  memory                   = 16384
  execution_role_arn       = aws_iam_role.this.arn
  task_role_arn            = aws_iam_role.this.arn

  container_definitions = jsonencode([
    {
      name       = "main-container"
      image      = "${var.ecr_repo_url}-${local.env}:${var.ecr_repo_image_tag}"
      essential  = true
      cpu        = 4096
      memory     = 16384
      aws_region = var.aws_region
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.this.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "${local.project}"
        }
      }
      environment = [
        {
          name  = "ENVIRONMENT"
          value = local.env
        }
      ]
    }
  ])

  depends_on = [
    aws_cloudwatch_log_group.this,
    aws_ecr_repository.this,
    aws_iam_role.this
  ]
}

resource "aws_iam_role" "this" {
  name = "${local.project}-ecs-task-execution-role-${local.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ECSTrustedRole"
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      },
    ]
  })

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]
}

data "aws_iam_policy_document" "ecs_task_execution_role" {
  statement {
    sid = "ECSAccess"
    actions = [
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:GetRepositoryPolicy",
      "ecr:DescribeRepositories",
      "ecr:ListImages",
      "ecr:DescribeImages",
      "ecr:BatchGetImage",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    sid = "SecretsManagers"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
      "secretsmanager:ListSecretVersionIds",
      "secretsmanager:ListSecrets"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    sid = "AthenaAccess"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:ListQueryExecutions",
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    sid = "S3Access"
    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:GetBucketLocation",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]
    effect = "Allow"
    resources = [
      "arn:aws:s3:::data-lakehouse-dev",
      "arn:aws:s3:::data-lakehouse-dev/*",
      "arn:aws:s3:::data-lakehouse-prod",
      "arn:aws:s3:::data-lakehouse-prod/*",
      "arn:aws:s3:::aws-athena-query-results",
      "arn:aws:s3:::aws-athena-query-results/*",
    ]
  }
}

resource "aws_iam_policy" "this" {
  name   = "${local.project}-ecs-task-execution-policy-${local.env}"
  policy = data.aws_iam_policy_document.ecs_task_execution_role.json
}

resource "aws_iam_role_policy_attachment" "this" {
  role       = aws_iam_role.this.name
  policy_arn = aws_iam_policy.this.arn
}
