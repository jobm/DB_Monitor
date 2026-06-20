output "postgres_fqdn" {
  description = "Fully qualified domain name of the PostgreSQL server"
  value = var.create_postgres_server ? (
    azurerm_postgresql_flexible_server.monitor[0].fqdn
  ) : null
}

output "postgres_connection_url" {
  description = "PostgreSQL connection URL for DB Monitor (asyncpg)"
  value = var.create_postgres_server ? (
    "postgresql+asyncpg://${var.postgres_admin_user}:${var.postgres_admin_password}@${azurerm_postgresql_flexible_server.monitor[0].fqdn}:5432/${var.postgres_database_name}"
  ) : null
  sensitive = true
}

output "postgres_database_name" {
  description = "Name of the monitor database"
  value       = var.postgres_database_name
}

output "postgres_admin_user" {
  description = "PostgreSQL admin username"
  value       = var.postgres_admin_user
}

# Convenience outputs for Helm chart values
output "helm_values" {
  description = "Minimal Helm values referencing the provisioned infrastructure"
  value = var.create_postgres_server ? {
    database = {
      host = azurerm_postgresql_flexible_server.monitor[0].fqdn
      port = 5432
      name = var.postgres_database_name
      user = var.postgres_admin_user
      existingSecret = "db-monitor-postgres-secret"
    }
  } : null
}
