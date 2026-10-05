mock_provider "aws" {
  mock_data "aws_region" {
    defaults = { region = "eu-west-1" }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::123456789012:role/mock" }
  }
  mock_resource "aws_lb" {
    defaults = { arn = "arn:aws:elasticloadbalancing:eu-west-1:123456789012:loadbalancer/app/mock/1" }
  }
  mock_resource "aws_lb_target_group" {
    defaults = { arn = "arn:aws:elasticloadbalancing:eu-west-1:123456789012:targetgroup/mock/1" }
  }
  mock_resource "aws_secretsmanager_secret" {
    defaults = { arn = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:mock" }
  }
  mock_resource "aws_ecs_task_definition" {
    defaults = { arn = "arn:aws:ecs:eu-west-1:123456789012:task-definition/mock:1" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{}" }
  }
}

variables {
  name_prefix          = "retail-data-platform-demo"
  log_group_name       = "/retail-data-platform-demo/serving"
  vpc_id               = "vpc-1"
  subnet_ids           = ["subnet-a", "subnet-b"]
  allowed_cidr         = "203.0.113.7/32"
  artifacts_bucket_arn = "arn:aws:s3:::art"
  image_tag            = "abc1234"
}

run "disabled_by_default" {
  command = apply # mocked provider: no API calls
  assert {
    condition     = length(aws_lb.serving) == 0 && length(aws_lb_target_group.serving) == 0 && length(aws_lb_listener.http) == 0 && length(aws_ecs_service.serving) == 0
    error_message = "ALB, target group, listener and service must be skipped unless enable_serving"
  }
  assert {
    condition     = aws_ecs_cluster.serving.name == "retail-data-platform-demo-serving" && aws_ecr_repository.serving.name == "retail-data-platform-demo-serving"
    error_message = "ECR repository and cluster always exist (CI pushes the image)"
  }
  assert {
    condition     = output.alb_dns_name == null && output.service_name == null
    error_message = "ALB/service outputs must be null when disabled"
  }
  assert {
    condition     = one(aws_ecr_repository.serving.image_scanning_configuration).scan_on_push
    error_message = "scan on push"
  }
  assert {
    condition     = jsondecode(aws_ecr_lifecycle_policy.serving.policy).rules[0].selection.countNumber == 5
    error_message = "keep 5 images"
  }
  assert {
    condition     = jsondecode(aws_ecs_task_definition.serving.container_definitions)[0].secrets[0].name == "MLFLOW_TRACKING_URI"
    error_message = "MLFLOW_TRACKING_URI from the secret"
  }
}

run "enabled" {
  command = apply
  variables {
    enable_serving = true
  }
  assert {
    condition     = length(aws_lb.serving) == 1 && length(aws_lb_listener.http) == 1 && length(aws_ecs_service.serving) == 1
    error_message = "enable_serving creates the ALB, listener and service"
  }
  assert {
    condition     = aws_ecs_service.serving[0].desired_count == 0
    error_message = "desired_count must default to 0"
  }
  assert {
    condition     = one(aws_lb_target_group.serving[0].health_check).path == "/health"
    error_message = "health check must hit /health"
  }
  assert {
    condition     = aws_vpc_security_group_ingress_rule.alb_http.cidr_ipv4 == "203.0.113.7/32"
    error_message = "ALB ingress must be the allowed CIDR"
  }
  assert {
    condition     = length(aws_lb.serving[0].name) <= 32
    error_message = "ALB name too long"
  }
  assert {
    condition     = output.service_name == "retail-data-platform-demo-serving"
    error_message = "service_name output when enabled"
  }
}

run "open_cidr_rejected" {
  command = plan
  variables {
    allowed_cidr = "0.0.0.0/0"
  }
  expect_failures = [var.allowed_cidr]
}
