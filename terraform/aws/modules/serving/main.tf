locals {
  name = "${var.name_prefix}-serving"
  # ELB names are capped at 32 characters
  lb_name = "${var.name_prefix}-svc"
  port    = 8000
}

data "aws_region" "current" {}

resource "aws_ecr_repository" "serving" {
  name                 = local.name
  image_tag_mutability = "IMMUTABLE"
  force_delete         = var.force_delete

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "serving" {
  repository = aws_ecr_repository.serving.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep the 5 most recent images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

# Value (the MLflow tracking URI) is set out of band with `aws secretsmanager put-secret-value`
# before scaling up, so it never lands in Terraform state.
resource "aws_secretsmanager_secret" "config" {
  name = "${local.name}/mlflow-tracking-uri"
  # demo: immediate delete so destroy + re-apply can reuse the name
  recovery_window_in_days = 0
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "execution_secret" {
  name = "read-config-secret"
  role = aws_iam_role.execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [aws_secretsmanager_secret.config.arn]
    }]
  })
}

resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "task" {
  name = "read-config-and-models"
  role = aws_iam_role.task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.config.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.artifacts_bucket_arn}/${var.model_prefix}*"]
      },
      {
        Effect    = "Allow"
        Action    = ["s3:ListBucket"]
        Resource  = [var.artifacts_bucket_arn]
        Condition = { StringLike = { "s3:prefix" = ["${var.model_prefix}*"] } }
      },
    ]
  })
}

resource "aws_ecs_cluster" "serving" {
  name = local.name
}

resource "aws_ecs_task_definition" "serving" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name      = "serving"
    image     = "${aws_ecr_repository.serving.repository_url}:${var.image_tag}"
    essential = true
    # same launch as the compose `serving` service (image ENTRYPOINT is the retail-ml CLI)
    entryPoint = ["sh", "-ec"]
    command = [
      "mkdir -p \"$PROMETHEUS_MULTIPROC_DIR\" && exec uvicorn retail_ml.serving.app:app --host 0.0.0.0 --port ${local.port} --workers 2"
    ]
    portMappings = [{ containerPort = local.port, protocol = "tcp" }]
    environment = [
      { name = "PROMETHEUS_MULTIPROCESS_DIR", value = "/tmp/prometheus" },
      { name = "PROMETHEUS_MULTIPROC_DIR", value = "/tmp/prometheus" },
    ]
    secrets = [{ name = "MLFLOW_TRACKING_URI", valueFrom = aws_secretsmanager_secret.config.arn }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = var.log_group_name
        awslogs-region        = data.aws_region.current.region
        awslogs-stream-prefix = "serving"
      }
    }
  }])
}

resource "aws_security_group" "alb" {
  name        = "${local.name}-alb"
  description = "Serving ALB: HTTP from the allowed CIDR only"
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  security_group_id = aws_security_group.alb.id
  description       = "HTTP from the allowed CIDR"
  cidr_ipv4         = var.allowed_cidr
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
}

resource "aws_vpc_security_group_egress_rule" "alb_to_task" {
  security_group_id            = aws_security_group.alb.id
  description                  = "To the serving tasks"
  referenced_security_group_id = aws_security_group.task.id
  ip_protocol                  = "tcp"
  from_port                    = local.port
  to_port                      = local.port
}

resource "aws_security_group" "task" {
  name        = "${local.name}-task"
  description = "Serving tasks: app port from the ALB only"
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "task_from_alb" {
  security_group_id            = aws_security_group.task.id
  description                  = "App port from the ALB"
  referenced_security_group_id = aws_security_group.alb.id
  ip_protocol                  = "tcp"
  from_port                    = local.port
  to_port                      = local.port
}

# ECR pull, Secrets Manager, S3 and the MLflow server (any port) over the public IP; no NAT cost
resource "aws_vpc_security_group_egress_rule" "task_out" {
  security_group_id = aws_security_group.task.id
  description       = "Outbound for image pull, secrets, S3 and MLflow"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_lb" "serving" {
  name                       = local.lb_name
  load_balancer_type         = "application"
  security_groups            = [aws_security_group.alb.id]
  subnets                    = var.subnet_ids
  drop_invalid_header_fields = true
}

resource "aws_lb_target_group" "serving" {
  name        = local.lb_name
  port        = local.port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = var.vpc_id

  health_check {
    path    = "/health"
    matcher = "200"
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.serving.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.serving.arn
  }
}

resource "aws_ecs_service" "serving" {
  name            = local.name
  cluster         = aws_ecs_cluster.serving.id
  task_definition = aws_ecs_task_definition.serving.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.serving.arn
    container_name   = "serving"
    container_port   = local.port
  }

  depends_on = [aws_lb_listener.http]
}
