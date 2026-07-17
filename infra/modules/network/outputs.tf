output "vpc_id" {
  description = "ID of the platform VPC."
  value       = aws_vpc.this.id
}

output "vpc_cidr" {
  description = "CIDR of the platform VPC."
  value       = aws_vpc.this.cidr_block
}

# AZ-keyed maps: the keys are static (from var.availability_zones), so consumers
# can for_each over them even though the ids are only known after apply.
output "public_subnets_by_az" {
  description = "Public subnet ids keyed by AZ."
  value       = { for az in var.availability_zones : az => aws_subnet.public[az].id }
}

output "private_subnets_by_az" {
  description = "Private subnet ids keyed by AZ."
  value       = { for az in var.availability_zones : az => aws_subnet.private[az].id }
}

output "public_subnet_ids" {
  description = "Public subnet ids (NAT gateways + internet-facing load balancers), ordered by AZ."
  value       = [for az in var.availability_zones : aws_subnet.public[az].id]
}

output "private_subnet_ids" {
  description = "Private subnet ids (EKS nodes + pods), ordered by AZ."
  value       = [for az in var.availability_zones : aws_subnet.private[az].id]
}
