variable "project_name" {
  description = "Nombre del proyecto"
  type        = string
}

variable "environment" {
  description = "Entorno de despliegue"
  type        = string
}

variable "region" {
  description = "Region AWS"
  type        = string
}

variable "vpc_id" {
  description = "ID de la VPC donde se despliega Redshift Serverless"
  type        = string
}

variable "private_subnet_ids" {
  description = "Subredes privadas utilizadas por Redshift Serverless"
  type        = list(string)
}

variable "kinesis_stream_arn" {
  description = "ARN del Kinesis Data Stream consumido por Redshift"
  type        = string
}

variable "lakehouse_bucket_arn" {
  description = "ARN del bucket S3 que contiene las tablas Iceberg"
  type        = string
}

variable "glue_database_name" {
  description = "Nombre de la base de datos Glue del Lakehouse"
  type        = string
}

variable "database_name" {
  description = "Nombre de la base de datos principal de Redshift Serverless"
  type        = string
}
