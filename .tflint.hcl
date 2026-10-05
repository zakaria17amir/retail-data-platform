config {
  call_module_type = "local"
}

plugin "terraform" {
  enabled = true
  preset  = "all"
}

# envs/demo/compute.tf keeps its own variables/outputs (split from main.tf so phase-7 tasks had disjoint files)
rule "terraform_standard_module_structure" {
  enabled = false
}

plugin "aws" {
  enabled = true
  version = "0.49.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}
