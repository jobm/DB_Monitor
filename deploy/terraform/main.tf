# DB Monitor — Companion Infrastructure
#
# This module provisions a PostgreSQL server suitable as the DB Monitor
# storage backend, plus optional Kafka topic resources.
#
# Usage:
#   module "db_monitor_infra" {
#     source = "github.com/jobm/DB_Monitor//deploy/terraform"
#
#     postgres_admin_password = var.postgres_admin_password
#     postgres_server_name    = "dbm-monitor"
#     resource_group_name     = azurerm_resource_group.main.name
#     location                = azurerm_resource_group.main.location
#   }

terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.0"
    }
  }
}

# ── PostgreSQL flexible server ───────────────────────────────────────

resource "azurerm_postgresql_flexible_server" "monitor" {
  count = var.create_postgres_server ? 1 : 0

  name                = var.postgres_server_name
  resource_group_name = var.resource_group_name
  location            = var.location

  administrator_login    = var.postgres_admin_user
  administrator_password = var.postgres_admin_password

  sku_name   = var.postgres_sku
  version    = var.postgres_version
  storage_mb = var.postgres_storage_mb

  backup_retention_days        = var.postgres_backup_retention_days
  geo_redundant_backup_enabled = var.postgres_geo_redundant_backup

  high_availability {
    mode                      = var.postgres_ha_mode
    standby_availability_zone = var.postgres_ha_standby_zone
  }

  maintenance_window {
    day_of_week  = var.maintenance_day
    start_hour   = var.maintenance_hour
    start_minute = 0
  }

  delegated_subnet_id = var.postgres_subnet_id
  private_dns_zone_id = var.postgres_private_dns_zone_id

  tags = var.tags
}

resource "azurerm_postgresql_flexible_server_database" "monitor" {
  count = var.create_postgres_server ? 1 : 0

  name      = var.postgres_database_name
  server_id = azurerm_postgresql_flexible_server.monitor[0].id
}

# ── PostgreSQL firewall rules ────────────────────────────────────────

resource "azurerm_postgresql_flexible_server_firewall_rule" "allowed_cidrs" {
  for_each = var.create_postgres_server ? var.postgres_allowed_cidrs : {}

  name             = "allow-${replace(each.key, ".", "-")}"
  server_id        = azurerm_postgresql_flexible_server.monitor[0].id
  start_ip_address = each.value.start
  end_ip_address   = each.value.end
}

# ── PostgreSQL configuration ─────────────────────────────────────────

resource "azurerm_postgresql_flexible_server_configuration" "wal_level" {
  count = var.create_postgres_server ? 1 : 0

  name      = "wal_level"
  server_id = azurerm_postgresql_flexible_server.monitor[0].id
  value     = "logical"
}

resource "azurerm_postgresql_flexible_server_configuration" "max_connections" {
  count = var.create_postgres_server ? 1 : 0

  name      = "max_connections"
  server_id = azurerm_postgresql_flexible_server.monitor[0].id
  value     = var.postgres_max_connections
}

resource "azurerm_postgresql_flexible_server_configuration" "shared_preload_libraries" {
  count = var.create_postgres_server ? 1 : 0

  name      = "shared_preload_libraries"
  server_id = azurerm_postgresql_flexible_server.monitor[0].id
  value     = "pg_stat_statements"
}

# ── Kafka topics (optional) ──────────────────────────────────────────

# If using Confluent Cloud or another managed Kafka, topic creation is
# handled by the provider's own tooling. This block is a reference for
# self-managed Kafka clusters that use Terraform to manage topics.
#
# resource "kafka_topic" "source_topics" {
#   for_each = toset(var.kafka_source_topics)
#
#   name               = each.value
#   partitions         = var.kafka_topic_partitions
#   replication_factor = var.kafka_topic_replication_factor
#
#   config = {
#     "retention.ms"    = var.kafka_topic_retention_ms
#     "cleanup.policy"  = "delete"
#   }
# }

# ── Key Vault for secrets (optional) ─────────────────────────────────

resource "azurerm_key_vault_secret" "postgres_connection_url" {
  count = var.create_postgres_server && var.create_key_vault_secrets ? 1 : 0

  name         = "db-monitor-postgres-url"
  value        = "postgresql+asyncpg://${var.postgres_admin_user}:${var.postgres_admin_password}@${azurerm_postgresql_flexible_server.monitor[0].fqdn}:5432/${var.postgres_database_name}"
  key_vault_id = var.key_vault_id
}

resource "azurerm_key_vault_secret" "jwt_secret" {
  count = var.create_key_vault_secrets ? 1 : 0

  name         = "db-monitor-jwt-secret"
  value        = var.jwt_secret
  key_vault_id = var.key_vault_id
}
