locals {
  azs = var.availability_zones

  # Mirror the pet's clean split: public /20s in the low range, private /20s in
  # the high range, with a gap between for future growth. cidrsubnet on a /16
  # with 4 new bits yields /20 blocks (netnum 0.. = 10.x.0.0/20, 10.x.16.0/20…).
  #   public  az i -> netnum i       (10.20.0.0/20,   10.20.16.0/20)
  #   private az i -> netnum i + 8   (10.20.128.0/20, 10.20.144.0/20)
  public_cidrs  = [for i in range(length(local.azs)) : cidrsubnet(var.vpc_cidr, 4, i)]
  private_cidrs = [for i in range(length(local.azs)) : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  # One NAT gateway per AZ (HA, matches the pet), or just the first AZ's when
  # single_nat_gateway is set (cheaper). Private route tables always point at the
  # NAT in their own AZ, falling back to the sole NAT in single mode.
  nat_azs = var.single_nat_gateway ? [local.azs[0]] : local.azs
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.name_prefix}-vpc"
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-igw"
  }
}

# Public subnets: host NAT gateways and internet-facing load balancers. Nodes do
# NOT run here (they are private-only), so auto-assign public IP stays off.
resource "aws_subnet" "public" {
  for_each = { for i, az in local.azs : az => i }

  vpc_id                  = aws_vpc.this.id
  availability_zone       = each.key
  cidr_block              = local.public_cidrs[each.value]
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.name_prefix}-public-${each.key}"
    # Lets the AWS cloud provider auto-discover subnets for internet-facing LBs.
    "kubernetes.io/role/elb" = "1"
  }

  # These subnets are shared across colors; each cluster stamps its own
  # kubernetes.io/cluster/<name> tag via a separate aws_ec2_tag resource. Without
  # this, any update here (e.g. a default_tags change) reconciles the full tag set
  # and strips those externally-managed per-color tags. Ignore tag drift so the
  # aws_ec2_tag resources own their keys and coexist. (Create still sets tags.)
  lifecycle {
    ignore_changes = [tags, tags_all]
  }
}

# Private subnets: EKS nodes and pods (VPC CNI hands out real VPC IPs, so these
# are sized /20 ~ 4091 usable each).
resource "aws_subnet" "private" {
  for_each = { for i, az in local.azs : az => i }

  vpc_id            = aws_vpc.this.id
  availability_zone = each.key
  cidr_block        = local.private_cidrs[each.value]

  tags = {
    Name = "${var.name_prefix}-private-${each.key}"
    # Lets the AWS cloud provider auto-discover subnets for internal LBs.
    "kubernetes.io/role/internal-elb" = "1"
  }

  # Shared across colors; see the note on aws_subnet.public. aws_ec2_tag resources
  # own the per-color kubernetes.io/cluster/<name> keys — ignore tag drift here so a
  # tag update never strips them.
  lifecycle {
    ignore_changes = [tags, tags_all]
  }
}

resource "aws_eip" "nat" {
  for_each = toset(local.nat_azs)
  domain   = "vpc"

  tags = {
    Name = "${var.name_prefix}-nat-${each.key}"
  }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_nat_gateway" "this" {
  for_each = toset(local.nat_azs)

  allocation_id = aws_eip.nat[each.key].id
  subnet_id     = aws_subnet.public[each.key].id

  tags = {
    Name = "${var.name_prefix}-nat-${each.key}"
  }

  depends_on = [aws_internet_gateway.this]
}

# One public route table shared by all public subnets: default route to the IGW.
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = {
    Name = "${var.name_prefix}-public"
  }
}

resource "aws_route_table_association" "public" {
  for_each = aws_subnet.public

  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}

# One private route table per AZ: default route to that AZ's NAT gateway (or the
# sole NAT gateway in single_nat_gateway mode).
resource "aws_route_table" "private" {
  for_each = aws_subnet.private

  vpc_id = aws_vpc.this.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this[var.single_nat_gateway ? local.nat_azs[0] : each.key].id
  }

  tags = {
    Name = "${var.name_prefix}-private-${each.key}"
  }
}

resource "aws_route_table_association" "private" {
  for_each = aws_subnet.private

  subnet_id      = each.value.id
  route_table_id = aws_route_table.private[each.key].id
}

# S3 gateway endpoint so image-layer and log pulls to S3 skip the NAT data path
# (matches the pet). Attached to the private route tables where the nodes live.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [for rt in aws_route_table.private : rt.id]

  tags = {
    Name = "${var.name_prefix}-s3"
  }
}
