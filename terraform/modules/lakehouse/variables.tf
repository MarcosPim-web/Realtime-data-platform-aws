variable "project_name" {
  type        = string
  description = "Nombre del proyecto"
}

variable "environment" {
  type        = string
  description = "Ambiente de despliegue"
}

variable "glue_database_name" {
  type        = string
  description = "Nombre de la base de datos de AWS Glue"

}