#include <Dictionaries/PostgreSQLDictionarySource.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Core/QualifiedTableName.h>
#include <Core/Settings.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Dictionaries/registerDictionaries.h>

#if USE_LIBPQXX
#include <Columns/ColumnString.h>
#include <Common/DateLUTImpl.h>
#include <Common/RemoteHostFilter.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Sources/PostgreSQLSource.h>
#include <Dictionaries/readInvalidateQuery.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/StoragePostgreSQL.h>
#include <Common/logger_useful.h>
#endif


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 postgresql_connection_attempt_timeout;
    extern const SettingsBool postgresql_connection_pool_auto_close_connection;
    extern const SettingsUInt64 postgresql_connection_pool_retries;
    extern const SettingsUInt64 postgresql_connection_pool_size;
    extern const SettingsUInt64 postgresql_connection_pool_wait_timeout;
    extern const SettingsSSLMode postgresql_connection_pool_ssl_mode;
    extern const SettingsString postgresql_connection_pool_ssl_root_cert;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}

static const ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> dictionary_allowed_keys = {
    "host", "port", "user", "password", "db", "database", "table", "schema", "background_reconnect",
    "update_field", "update_lag", "invalidate_query", "query", "where", "name", "priority",
    "sslmode", "sslrootcert", "sslcert", "sslkey", "sslrootcert_pem", "sslcert_pem", "sslkey_pem"};

#if USE_LIBPQXX
/// The source configuration of a dictionary created with a DDL query comes from the query itself, so
/// it may not name files for the server to open: the server reads them with its own privileges, and a
/// user who cannot read a certificate and key must not be able to authenticate with them. The
/// contents can be passed in `sslrootcert_pem`, `sslcert_pem` and `sslkey_pem` instead.
/// Dictionaries defined in server configuration files are written by an operator and keep using paths.
static void checkNoSSLPaths(const Poco::Util::AbstractConfiguration & config, const std::string & prefix)
{
    static const std::initializer_list<std::pair<std::string_view, std::string_view>> keys
        = {{"sslrootcert", "sslrootcert_pem"}, {"sslcert", "sslcert_pem"}, {"sslkey", "sslkey_pem"}};

    for (const auto & [key, contents_key] : keys)
    {
        if (config.has(prefix + "." + std::string(key)))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`{}` cannot be specified in a dictionary created with a DDL query. "
                "Pass the contents of the file in `{}` instead",
                key, contents_key);
    }
}
#endif

#if USE_LIBPQXX

static const UInt64 max_block_size = 8192;

namespace
{
    ExternalQueryBuilder makeExternalQueryBuilder(const DictionaryStructure & dict_struct, const String & schema, const String & table, const String & query, const String & where)
    {
        QualifiedTableName qualified_name{schema, table};

        if (qualified_name.database.empty() && !qualified_name.table.empty())
            qualified_name = QualifiedTableName::parseFromString(qualified_name.table);

        /// Do not need db because it is already in a connection string.
        return {dict_struct, "", qualified_name.database, qualified_name.table, query, where, IdentifierQuotingStyle::DoubleQuotesPostgreSQL};
    }
}


PostgreSQLDictionarySource::PostgreSQLDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    postgres::PoolWithFailoverPtr pool_,
    SharedHeader sample_block_)
    : dict_struct(dict_struct_)
    , configuration(configuration_)
    , pool(std::move(pool_))
    , sample_block(sample_block_)
    , log(getLogger("PostgreSQLDictionarySource"))
    , query_builder(makeExternalQueryBuilder(dict_struct, configuration.schema, configuration.table, configuration.query, configuration.where))
    , load_all_query(query_builder.composeLoadAllQuery())
{
}


/// copy-constructor is provided in order to support cloneability
PostgreSQLDictionarySource::PostgreSQLDictionarySource(const PostgreSQLDictionarySource & other)
    : dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , pool(other.pool)
    , sample_block(other.sample_block)
    , log(getLogger("PostgreSQLDictionarySource"))
    , query_builder(makeExternalQueryBuilder(dict_struct, configuration.schema, configuration.table, configuration.query, configuration.where))
    , load_all_query(query_builder.composeLoadAllQuery())
    , update_time(other.update_time)
    , invalidate_query_response(other.invalidate_query_response)
{
}


BlockIO PostgreSQLDictionarySource::loadAll()
{
    LOG_TRACE(log, fmt::runtime(load_all_query));
    BlockIO io;
    io.pipeline = loadBase(load_all_query);
    return io;
}


BlockIO PostgreSQLDictionarySource::loadUpdatedAll()
{
    auto load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, fmt::runtime(load_update_query));
    BlockIO io;
    io.pipeline = loadBase(load_update_query);
    return io;
}

BlockIO PostgreSQLDictionarySource::loadIds(const VectorWithMemoryTracking<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    BlockIO io;
    io.pipeline = loadBase(query);
    return io;
}


BlockIO PostgreSQLDictionarySource::loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    BlockIO io;
    io.pipeline = loadBase(query);
    return io;
}


QueryPipeline PostgreSQLDictionarySource::loadBase(const String & query)
{
    return QueryPipeline(std::make_shared<PostgreSQLSource<>>(pool->get(), query, sample_block, max_block_size));
}


bool PostgreSQLDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        return invalidate_query_response.updateAndCheckModified(response);
    }
    return true;
}


std::string PostgreSQLDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block invalidate_sample_block;
    ColumnPtr column(ColumnString::create());
    invalidate_sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));

    QueryPipeline pipeline(std::make_unique<PostgreSQLSource<>>(pool->get(), request, std::make_shared<const Block>(std::move(invalidate_sample_block)), 1));
    return readInvalidateQuery(pipeline);
}


bool PostgreSQLDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}


std::string PostgreSQLDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - configuration.update_lag;
        std::string str_time = DateLUT::instance().timeToString(hr_time);
        update_time = std::chrono::system_clock::now();
        return query_builder.composeUpdateQuery(configuration.update_field, str_time);
    }

    update_time = std::chrono::system_clock::now();
    return query_builder.composeLoadAllQuery();
}


bool PostgreSQLDictionarySource::supportsSelectiveLoad() const
{
    return true;
}


DictionarySourcePtr PostgreSQLDictionarySource::clone() const
{
    return std::make_shared<PostgreSQLDictionarySource>(*this);
}


std::string PostgreSQLDictionarySource::toString() const
{
    const auto & where = configuration.where;
    return "PostgreSQL: " + configuration.db + '.' + configuration.table + (where.empty() ? "" : ", where: " + where);
}

#endif

void registerDictionarySourcePostgreSQL(DictionarySourceFactory & factory);
void registerDictionarySourcePostgreSQL(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const String & /*name*/,
                                 const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr context,
                                 const std::string & /* default_database */,
                                 [[maybe_unused]] bool created_from_ddl) -> DictionarySourcePtr
    {
#if USE_LIBPQXX
        const auto settings_config_prefix = config_prefix + ".postgresql";
        const auto & settings = context->getSettingsRef();

        std::optional<PostgreSQLDictionarySource::Configuration> dictionary_configuration;

        /// Every key here comes from the `CREATE DICTIONARY` query, including the keys that override a
        /// named collection.
        if (created_from_ddl)
            checkNoSSLPaths(config, settings_config_prefix);

        auto named_collection = created_from_ddl ? tryGetNamedCollectionWithOverrides(config, settings_config_prefix, context) : nullptr;
        if (!named_collection)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "PostgreSQL dictionary source configuration must use a named collection");

        /// A dictionary source reads either a remote table or a `query` that the dictionary composes and
        /// runs itself, so the table is only required when there is no `query`. The name is read from the
        /// named collection directly: the shared parser wraps it in a `TableNameOrQuery`, whose `QUERY`
        /// type is the table engine's passthrough query and means something else than a dictionary
        /// source `query`.
        const bool has_query = named_collection->has("query");

        StoragePostgreSQL::Configuration common_configuration = StoragePostgreSQL::processNamedCollectionResult(
            *named_collection, /*storage_settings=*/nullptr, context, dictionary_allowed_keys, /*require_table=*/!has_query);

        dictionary_configuration.emplace(PostgreSQLDictionarySource::Configuration{
            .db = common_configuration.database,
            .schema = common_configuration.schema,
            .table = named_collection->getOrDefault<String>("table", ""),
            .query = named_collection->getOrDefault<String>("query", ""),
            .where = named_collection->getOrDefault<String>("where", ""),
            .invalidate_query = named_collection->getOrDefault<String>("invalidate_query", ""),
            .update_field = named_collection->getOrDefault<String>("update_field", ""),
            .update_lag = named_collection->getOrDefault<UInt64>("update_lag", 1),
        });

        for (const auto & [host, port] : common_configuration.addresses)
            context->getRemoteHostFilter().checkHostAndPort(host, toString(port));


        auto pool = std::make_shared<postgres::PoolWithFailover>(
            common_configuration,
            settings[Setting::postgresql_connection_pool_size],
            settings[Setting::postgresql_connection_pool_wait_timeout],
            settings[Setting::postgresql_connection_pool_retries],
            settings[Setting::postgresql_connection_pool_auto_close_connection],
            settings[Setting::postgresql_connection_attempt_timeout],
            static_cast<postgres::SSLMode>(settings[Setting::postgresql_connection_pool_ssl_mode]),
            static_cast<String>(settings[Setting::postgresql_connection_pool_ssl_root_cert]),
            named_collection->getOrDefault<bool>("background_reconnect", false));


        return std::make_unique<PostgreSQLDictionarySource>(dict_struct, dictionary_configuration.value(), pool, std::make_shared<const Block>(sample_block));
#else
        (void)dict_struct;
        (void)config;
        (void)config_prefix;
        (void)sample_block;
        (void)context;
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary source of type `postgresql` is disabled because ClickHouse was built without postgresql support.");
#endif
    };

    factory.registerSource("postgresql", create_table_source, Documentation{
        .description = R"DOCS_MD(
# PostgreSQL dictionary source

Example of settings:

<Tabs>
<Tab title="DDL">

```sql
SOURCE(POSTGRESQL(
    port 5432
    host 'postgresql-hostname'
    user 'postgres_user'
    password 'postgres_password'
    db 'db_name'
    table 'table_name'
    replica(host 'example01-1' port 5432 priority 1)
    replica(host 'example01-2' port 5432 priority 2)
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

</Tab>
<Tab title="Configuration file">

```xml
<source>
  <postgresql>
      <host>postgresql-hostname</host>
      <port>5432</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </postgresql>
</source>
```

</Tab>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `host` | The host on the PostgreSQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `port` | The port on the PostgreSQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `user` | Name of the PostgreSQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `password` | Password of the PostgreSQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `replica` | Section of replica configurations. There can be multiple sections. |
| `replica/host` | The PostgreSQL host. |
| `replica/port` | The PostgreSQL port. |
| `replica/priority` | The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority. |
| `db` | Name of the database. |
| `table` | Name of the table. |
| `where` | The selection criteria. The syntax for conditions is the same as for `WHERE` clause in PostgreSQL. For example, `id > 10 AND id < 20`. Optional. |
| `invalidate_query` | Query for checking the dictionary status. Optional. Read more in the section [Refreshing dictionary data using LIFETIME](/reference/statements/create/dictionary/lifetime). |
| `background_reconnect` | Reconnect to replica in background if connection fails. Optional. |
| `query` | The custom query. Optional. |
| `sslmode` | TLS/SSL mode passed to `libpq`: `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full`. When unset, the `libpq` default of `prefer` applies. Optional. |
| `sslrootcert_pem` | Contents of the CA certificate that the PostgreSQL server certificate is verified against. Optional. |
| `sslcert_pem` | Contents of the client certificate, for certificate-based authentication. Optional. |
| `sslkey_pem` | Contents of the private key belonging to `sslcert_pem`. Optional. |
| `sslrootcert`, `sslcert`, `sslkey` | The same credentials as paths to files on the server. Only allowed for a dictionary defined in a server configuration file, or through a named collection defined there, see below. Optional. |

<Note>
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
</Note>

<Note>
`sslrootcert`, `sslcert` and `sslkey` name files that the server opens with its own privileges, so they are only accepted for a dictionary defined in a server configuration file, or through a named collection defined there. A dictionary created with a `CREATE DICTIONARY` query must pass the contents instead, in `sslrootcert_pem`, `sslcert_pem` and `sslkey_pem`. Those values are masked in logs and in `SHOW` queries, the same way passwords are.
</Note>
)DOCS_MD"
#if !USE_LIBPQXX
            "\n\nCurrently unavailable, because this ClickHouse build does not include PostgreSQL support."
#endif
        ,
        .syntax = "SOURCE(POSTGRESQL(host 'host' port 5432 user 'user' password '' db 'db' table 'table'))",
        .related = {"mysql", "clickhouse"}});
}

}
