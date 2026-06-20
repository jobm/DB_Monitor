# ── PostgreSQL configuration ─────────────────────────────────────────

variable "create_postgres_server" {
  description = "Whether to create a new PostgreSQL flexible server"
  type        = bool
  default     = true
}

variable "postgres_server_name" {
  description = "Name of the PostgreSQL flexible server"
  type        = string
  default     = "db-monitor"
}

variable "resource_group_name" {
  description = "Azure resource group name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "postgres_admin_user" {
  description = "PostgreSQL admin username"
  type        = string
  default     = "dbmonitor"
}

variable "postgres_admin_password" {
  description = "PostgreSQL admin password"
  type        = string
  sensitive   = true
}

variable "postgres_sku" {
  description = "PostgreSQL SKU (GP_Standard_D2s_v3, GP_Standard_D4s_v3, etc.)"
  type        = string
  default     = "GP_Standard_D2s_v3"
}

variable "postgres_version" {
  description = "PostgreSQL major version"
  type        = string
  default     = "15"
}

variable "postgres_storage_mb" {
  description = "PostgreSQL storage size in MB"
  type        = number
  default     = 131072 # 128 GB
}

variable "postgres_backup_retention_days" {
  description = "Backup retention in days"
  type        = number
  default     = 7
}

variable "postgres_geo_redundant_backup" {
  description = "Enable geo-redundant backups"
  type        = bool
  default     = false
}

variable "postgres_ha_mode" {
  description = "High availability mode (Disabled, SameZone, ZoneRedundant)"
  type        = string
  default     = "Disabled"
}

variable "postgres_ha_standby_zone" {
  description = "Standby availability zone for HA"
  type        = string
  default     = "2"
}

variable "maintenance_day" {
  description = "Day of week for maintenance (0=Sunday)"
  type        = number
  default     = 0
}

variable "maintenance_hour" {
  description = "Hour for maintenance window (0-23)"
  type        = number
  default     = 3
}

variable "postgres_subnet_id" {
  description = "Subnet ID for PostgreSQL delegated subnet"
  type        = string
  default     = ""
}

variable "postgres_private_dns_zone_id" {
  description = "Private DNS zone ID for PostgreSQL"
  type        = string
  default     = ""
}

variable "postgres_database_name" {
  description = "Name of the monitor database"
  type        = string
  default     = "postgres"
}

variable "postgres_allowed_cidrs" {
  description = "Map of CIDR ranges allowed to access PostgreSQL"
  type = map(object({
    start = string
    end   = string
  }))
  default = {}
}

variable "postgres_max_connections" {
  description = "Maximum number of PostgreSQL connections"
  type        = string
  default     = "200"
}

# ── Kafka configuration ──────────────────────────────────────────────

variable "kafka_source_topics" {
  description = "List of Kafka topics to create for source databases"
  type        = list(string)
  default     = []
}

variable "kafka_topic_partitions" {
  description = "Number of partitions per Kafka topic"
  type        = number
  default     = 6
}

variable "kafka_topic_replication_factor" {
  description = "Replication factor for Kafka topics"
  type        = number
  default     = 3
}

variable "kafka_topic_retention_ms" {
  description = "Kafka topic retention in milliseconds"
  type        = string
  default     = "604800000" # 7 days
}

# ── Key Vault ────────────────────────────────────────────────────────

variable "create_key_vault_secrets" {
  description = "Store secrets in Azure Key Vault"
  type        = bool
  default     = false
}

variable "key_vault_id" {
  description = "Azure Key Vault ID for storing secrets"
  type        = string
  default     = ""
}

variable "jwt_secret" {
  description = "JWT signing secret for DB Monitor"
  type        = string
  sensitive   = true
  default     = ""
}

# ── Common ────────────────────────────────────────────────────────────

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}
