data "aws_caller_identity" "current" {}

# ==============================================================================
# IAM ROLE
# Permite a Redshift Serverless consumir Kinesis y consultar Glue + Iceberg.
# ==============================================================================

resource "aws_iam_role" "redshift" {
  name = "${var.project_name}-${var.environment}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = [
            "redshift.amazonaws.com",
            "redshift-serverless.amazonaws.com"
          ]
        }

        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_policy" "redshift" {
  name        = "${var.project_name}-${var.environment}-redshift-policy"
  description = "Permisos de Redshift para Kinesis Streaming Ingestion y Lakehouse Iceberg"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "ReadKinesisStream"
        Effect = "Allow"

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]

        Resource = var.kinesis_stream_arn
      },

      {
        Sid    = "ListKinesisStreams"
        Effect = "Allow"

        Action = [
          "kinesis:ListStreams"
        ]

        Resource = "*"
      },

      {
        Sid    = "DecryptKinesis"
        Effect = "Allow"

        Action = [
          "kms:Decrypt"
        ]

        Resource = "*"

        Condition = {
          StringEquals = {
            "kms:ViaService" = "kinesis.${var.region}.amazonaws.com"
          }
        }
      },

      {
        Sid    = "ReadGlueDatabases"
        Effect = "Allow"

        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases"
        ]

        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/*"
        ]
      },
      {
        Sid    = "ReadGlueLakehouseTables"
        Effect = "Allow"

        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition"
        ]

        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*"
        ]
      },

      {
        Sid    = "ReadLakehouseBucket"
        Effect = "Allow"

        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]

        Resource = var.lakehouse_bucket_arn
      },

      {
        Sid    = "ReadIcebergObjects"
        Effect = "Allow"

        Action = [
          "s3:GetObject"
        ]

        Resource = "${var.lakehouse_bucket_arn}/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "redshift" {
  role       = aws_iam_role.redshift.name
  policy_arn = aws_iam_policy.redshift.arn
}

# ==============================================================================
# SECURITY GROUPS
# ==============================================================================

resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-${var.environment}-redshift-sg"
  description = "Security group para Redshift Serverless"
  vpc_id      = var.vpc_id

  egress {
    description = "Salida HTTPS hacia servicios AWS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-redshift-sg"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_security_group" "kinesis_endpoint" {
  name        = "${var.project_name}-${var.environment}-kinesis-endpoint-sg"
  description = "Permite a Redshift acceder al endpoint privado de Kinesis"
  vpc_id      = var.vpc_id

  ingress {
    description     = "HTTPS desde Redshift Serverless"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.redshift.id]
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-kinesis-endpoint-sg"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ==============================================================================
# KINESIS INTERFACE VPC ENDPOINT
# Mantiene Redshift -> Kinesis dentro de la red de AWS.
# ==============================================================================

resource "aws_vpc_endpoint" "kinesis" {
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.region}.kinesis-streams"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.kinesis_endpoint.id]
  private_dns_enabled = true

  tags = {
    Name        = "${var.project_name}-${var.environment}-kinesis-vpce"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ==============================================================================
# REDSHIFT SERVERLESS
# ==============================================================================

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name = "${var.project_name}-${var.environment}"

  db_name               = var.database_name
  manage_admin_password = true

  iam_roles = [
    aws_iam_role.redshift.arn
  ]

  default_iam_role_arn = aws_iam_role.redshift.arn

  log_exports = [
    "userlog",
    "connectionlog",
    "useractivitylog"
  ]

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_redshiftserverless_workgroup" "main" {
  workgroup_name = "${var.project_name}-${var.environment}-wg"
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name

  base_capacity = 4
  max_capacity  = 4

  publicly_accessible = false

  subnet_ids = var.private_subnet_ids

  security_group_ids = [
    aws_security_group.redshift.id
  ]

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ==============================================================================
# COST GUARD
# Corta compute si el entorno supera 8 RPU-hours en un dia.
# ==============================================================================

resource "aws_redshiftserverless_usage_limit" "daily_compute" {
  resource_arn  = aws_redshiftserverless_workgroup.main.arn
  usage_type    = "serverless-compute"
  amount        = 8
  period        = "daily"
  breach_action = "deactivate"
}
