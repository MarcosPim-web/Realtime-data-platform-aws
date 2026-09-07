variable "region" {
  description = "Región de AWS donde se crearán los recursos"
  type        = string
}

variable "project_name" {
  description = "Nombre del proyecto de plataforma de datos"
  type        = string
}

variable "environment" {
  description = "Entorno en el que se desplegará la infraestructura"
  type        = string
}

variable "vpc_cidr" {
  description = "Rango CIDR de la VPC"
  type        = string
}

variable "bucket_name" {
  description = "Nombre del bucket S3 utilizado para almacenar los datos de la capa Raw/Bronze"
  type        = string
}

variable "bucket_prefix" {
  description = "Prefijo del bucket al que puede acceder el rol de procesamiento"
  type        = string
}

variable "kinesis_shard_count" {
  description = "Cantidad de shards del stream de Kinesis"
  type        = number
}

variable "flink_start_application" {
  description = "Indica si Managed Flink debe iniciar automaticamente luego del despliegue"
  type        = bool
}

variable "flink_checkpoint_interval_ms" {
  description = "Intervalo entre checkpoints de Flink en milisegundos"
  type        = number
}

variable "flink_min_pause_between_checkpoints_ms" {
  description = "Pausa minima entre checkpoints de Flink en milisegundos"
  type        = number
}

variable "flink_parallelism" {
  description = "Paralelismo de la aplicacion Flink"
  type        = number
}

variable "flink_parallelism_per_kpu" {
  description = "Paralelismo por KPU de Managed Flink"
  type        = number
}

variable "glue_database_name" {
  description = "Nombre de la base de datos Glue del Lakehouse"
  type        = string
}

variable "redshift_database_name" {
  description = "Nombre de la base de datos principal de Redshift Serverless"
  type        = string
}
