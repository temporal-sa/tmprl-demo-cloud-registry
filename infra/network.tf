# One shared VPC hosts every cluster color (blue/green). Network defaults
# (10.20.0.0/16, two us-west-1 AZs, one NAT per AZ) live in the module.
module "network" {
  source = "./modules/network"

  name_prefix        = var.name_prefix
  region             = var.region
  single_nat_gateway = var.single_nat_gateway
}
