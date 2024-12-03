#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <pqxx/pqxx>
#include <Core/Types.h>
#include "Connection.h"
#include <Common/Exception.h>
#include "PoolWithFailover.h"

namespace pqxx
{
    using ReadTransaction = pqxx::read_transaction;
    using ReplicationTransaction = pqxx::transaction<isolation_level::repeatable_read, write_policy::read_only>;
}

namespace postgres
{
using SSLMode = DB::SSLMode;

ConnectionInfo formatConnectionString(
    String dbname,
    String host,
    UInt16 port,
    String user,
    String password,
    UInt64 timeout = POSTGRESQL_POOL_DEFAULT_CONNECT_TIMEOUT_SEC,
    std::optional<SSLMode> ssl_mode = POSTGRESQL_POOL_DEFAULT_SSL_MODE,
    String ssl_root_cert = POSTGRESQL_POOL_DEFAULT_SSL_ROOT_CERT);

String getConnectionForLog(const String & host, UInt16 port);

String formatNameForLogs(const String & postgres_database_name, const String & postgres_table_name);

}

#endif
