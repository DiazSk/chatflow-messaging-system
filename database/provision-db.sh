#!/usr/bin/env bash
# ============================================================
# provision-db.sh — ChatFlow Messaging System
#
# Provisions MySQL 8 on Amazon Linux 2023:
#   - Installs and enables mysqld
#   - Resets the temporary root password
#   - Creates the application database and user
#   - Loads 01-init-schema.sql (squashed schema)
#   - Applies production InnoDB tuning (1 GB buffer pool, etc.)
#   - Verifies the loaded schema
#
# Required env:
#   DB_PASSWORD    Application user password (also reused as the new root password).
#
# Optional env (override defaults):
#   DB_NAME        Default: chatflow
#   DB_USER        Default: chatflow
# ============================================================

set -euo pipefail

DB_NAME="${DB_NAME:-chatflow}"
DB_USER="${DB_USER:-chatflow}"
DB_PASSWORD="${DB_PASSWORD:?DB_PASSWORD must be set in the environment}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCHEMA_FILE="$SCRIPT_DIR/01-init-schema.sql"

if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "ERROR: schema file not found at $SCHEMA_FILE" >&2
    exit 1
fi

echo "==> Installing MySQL 8 on Amazon Linux 2023"
sudo dnf install -y https://dev.mysql.com/get/mysql80-community-release-el9-5.noarch.rpm
sudo dnf install -y mysql-community-server mysql-community-client

sudo systemctl start mysqld
sudo systemctl enable mysqld

echo "==> Resetting root password and provisioning ${DB_USER}@${DB_NAME}"
TEMP_PASSWORD=$(sudo grep 'temporary password' /var/log/mysqld.log | awk '{print $NF}' | tail -n1)

mysql --connect-expired-password -u root -p"$TEMP_PASSWORD" <<SQL
ALTER USER 'root'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';

CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';
CREATE USER IF NOT EXISTS '${DB_USER}'@'%'         IDENTIFIED BY '${DB_PASSWORD}';

CREATE DATABASE IF NOT EXISTS ${DB_NAME};

GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'%';
FLUSH PRIVILEGES;
SQL

echo "==> Loading schema from $SCHEMA_FILE"
mysql -u "${DB_USER}" -p"${DB_PASSWORD}" "${DB_NAME}" < "$SCHEMA_FILE"

echo "==> Applying production InnoDB tuning"
sudo tee /etc/my.cnf.d/chatflow-tuning.cnf > /dev/null <<'MYCNF'
[mysqld]
# Production buffer pool: 1 GB resident working set holds the indexed
# hot path of `messages` plus the summary tables in memory.
innodb_buffer_pool_size            = 1G

# Write throughput tuning
innodb_flush_log_at_trx_commit     = 2
innodb_flush_method                = O_DIRECT
innodb_log_buffer_size             = 64M
innodb_write_io_threads            = 4
innodb_read_io_threads             = 4

# Batch insert tuning
innodb_autoinc_lock_mode           = 2
bulk_insert_buffer_size            = 64M

# Connection settings
max_connections                    = 100
wait_timeout                       = 600
interactive_timeout                = 600
thread_cache_size                  = 16

# Slow query observability
slow_query_log                     = 1
slow_query_log_file                = /var/log/mysql/slow.log
long_query_time                    = 1

# Binary logging disabled (no replication target).
skip-log-bin
MYCNF

sudo mkdir -p /var/log/mysql
sudo chown mysql:mysql /var/log/mysql
sudo systemctl restart mysqld

echo "==> Verifying schema"
mysql -u "${DB_USER}" -p"${DB_PASSWORD}" "${DB_NAME}" -e "SHOW TABLES;"

echo
echo "==> Provisioning complete"
echo "    database: ${DB_NAME}"
echo "    user:     ${DB_USER}"
