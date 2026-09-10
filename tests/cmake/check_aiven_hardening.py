import argparse
import json
from pathlib import Path


EXCLUDED_SOURCES = (
    "src/Functions/FunctionFile.cpp",
    "src/Functions/catboostEvaluate.cpp",
    "src/Functions/getClientHTTPHeader.cpp",
    "src/Functions/UserDefined/UserDefinedExecutableFunction.cpp",
    "src/Processors/Sources/ShellCommandSource.cpp",
    "src/Storages/StorageExecutable.cpp",
    "src/Storages/StorageArrowFlight.cpp",
    "src/Storages/System/StorageSystemModels.cpp",
    "src/BridgeHelper/CatBoostLibraryBridgeHelper.cpp",
    "src/Databases/DatabaseBackup.cpp",
    "src/Databases/DatabaseFilesystem.cpp",
    "src/Server/PostgreSQLHandler.cpp",
    "src/Server/PostgreSQLHandlerFactory.cpp",
    "src/Processors/Formats/InputFormatErrorsLogger.cpp",
    "src/Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.cpp",
    "src/TableFunctions/TableFunctionExecutable.cpp",
    "src/TableFunctions/TableFunctionFile.cpp",
    "src/TableFunctions/TableFunctionFileCluster.cpp",
    "src/TableFunctions/TableFunctionMongoDB.cpp",
    "src/TableFunctions/TableFunctionRedis.cpp",
    "src/TableFunctions/TableFunctionArrowFlight.cpp",
    "src/TableFunctions/TableFunctionYtsaurus.cpp",
    "src/TableFunctions/ITableFunctionXDBC.cpp",
    "src/Dictionaries/ExecutableDictionarySource.cpp",
    "src/Dictionaries/ExecutablePoolDictionarySource.cpp",
    "src/Dictionaries/LibraryDictionarySource.cpp",
    "src/Dictionaries/FileDictionarySource.cpp",
    "src/Dictionaries/MongoDBDictionarySource.cpp",
    "src/Dictionaries/RedisDictionarySource.cpp",
    "src/Dictionaries/XDBCDictionarySource.cpp",
    "src/Dictionaries/YTsaurusDictionarySource.cpp",
    "src/Dictionaries/YAMLRegExpTreeDictionarySource.cpp",
    "contrib/librdkafka/src/rdkafka_mock.c",
    "contrib/librdkafka/src/rdkafka_mock_cgrp.c",
    "contrib/librdkafka/src/rdkafka_mock_handlers.c",
    "contrib/azure/sdk/identity/azure-identity/src/default_azure_credential.cpp",
    "contrib/azure/sdk/identity/azure-identity/src/managed_identity_credential.cpp",
    "contrib/azure/sdk/identity/azure-identity/src/managed_identity_source.cpp",
    "contrib/azure/sdk/identity/azure-identity/src/workload_identity_credential.cpp",
)

EXCLUDED_DIRECTORIES = (
    "src/Server/SSH/",
    "src/Server/ACME/",
    "src/Storages/ArrowFlight/",
    "src/Storages/FileLog/",
    "programs/local/",
    "programs/disks/",
    "programs/git-import/",
    "programs/static-files-disk-uploader/",
    "programs/su/",
)

RETAINED_SOURCES = (
    "programs/client/Client.cpp",
    "programs/server/Server.cpp",
    "src/Server/MySQLHandler.cpp",
    "src/Server/HTTPHandler.cpp",
    "src/Server/TCPHandler.cpp",
    "src/Server/ArrowFlightHandler.cpp",
    "src/Storages/Kafka/KafkaConfigLoader.cpp",
    "src/TableFunctions/TableFunctionRemote.cpp",
    "src/TableFunctions/TableFunctionURL.cpp",
    "src/TableFunctions/TableFunctionInput.cpp",
    "src/TableFunctions/TableFunctionObjectStorage.cpp",
    "src/Dictionaries/HTTPDictionarySource.cpp",
    "src/Dictionaries/ClickHouseDictionarySource.cpp",
    "src/Storages/MergeTree/MergeTreeData.cpp",
    "src/Storages/Freeze.cpp",
    "contrib/librdkafka-cmake/mock_disabled.c",
    "contrib/azure/sdk/identity/azure-identity/src/client_secret_credential.cpp",
)

DISABLED_MACROS = (
    "ENABLE_CATBOOST", "ENABLE_SQL_PROCESS_CONTROL", "ENABLE_SQL_SYNC_FILE_CACHE",
    "ENABLE_CLIENT_HTTP_HEADER", "ENABLE_POSTGRESQL_SERVER", "ENABLE_HTTP_AUTHENTICATION",
    "ENABLE_SQL_IMPERSONATION", "ENABLE_SQL_SECURITY_NONE", "ENABLE_KAFKA_MOCKS",
    "ENABLE_INPUT_FORMAT_ERROR_FILES", "ENABLE_WEB_OBJECT_STORAGE", "ENABLE_ACME",
    "ENABLE_CRASH_REPORTS", "ENABLE_REMOTE_SYSLOG", "ENABLE_GRAPHITE",
    "ENABLE_AMBIENT_AWS_CREDENTIALS", "ENABLE_AZURE_IDENTITY", "ENABLE_GCP_OAUTH",
    "ENABLE_ZONE_AUTODETECTION", "ENABLE_WEAK_PASSWORD_METHODS",
    "USE_WASMEDGE", "USE_WASMTIME", "USE_SSH", "USE_LDAP", "USE_LIBURING",
    "REGISTER_EXECUTABLE_UDF", "REGISTER_BACKUP_RESTORE", "REGISTER_CUSTOM_DISK",
    "REGISTER_ARROWFLIGHT_FUNCTION", "REGISTER_ARROWFLIGHT_TABLE_ENGINE", "REGISTER_REMOTE_FUNCTION",
)

RETAINED_MACROS = (
    "USE_AWS_S3", "USE_AZURE_BLOB_STORAGE", "USE_RDKAFKA", "USE_MYSQL", "USE_LIBPQXX",
    "USE_GRPC", "USE_ARROWFLIGHT", "USE_PARQUET", "USE_PROTOBUF", "USE_AVRO",
    "USE_EMBEDDED_COMPILER", "USE_SSL", "REGISTER_URL_FUNCTION", "REGISTER_URL_TABLE_ENGINE",
    "REGISTER_URL_CLUSTER_FUNCTION", "REGISTER_DICTIONARY_SOURCE_HTTP", "REGISTER_DICTIONARY_SOURCE_CLICKHOUSE",
    "REGISTER_OBJECT_STORAGE_FUNCTION", "REGISTER_OBJECT_STORAGE_TABLE_ENGINE", "REGISTER_DATALAKE_FUNCTION",
    "REGISTER_KEEPER_MAP_TABLE_ENGINE", "REGISTER_TIMESERIES_FUNCTION", "REGISTER_TIMESERIES_TABLE_ENGINE",
)


def check(build_dir):
    repo_root = Path(__file__).resolve().parents[2]
    entries = json.loads((build_dir / "compile_commands.json").read_text())
    sources = set()
    for entry in entries:
        source = Path(entry["file"])
        if not source.is_absolute():
            source = Path(entry["directory"]) / source
        try:
            sources.add(source.relative_to(repo_root).as_posix())
        except ValueError:
            continue

    unexpected = set(EXCLUDED_SOURCES) & sources
    unexpected.update(
        source
        for source in sources
        if source.startswith(EXCLUDED_DIRECTORIES)
    )
    missing = set(RETAINED_SOURCES) - sources
    if unexpected or missing:
        raise RuntimeError(
            f"Forbidden sources still compiled: {sorted(unexpected)}\n"
            f"Required sources missing: {sorted(missing)}"
        )

    macros = {}
    for line in (build_dir / "includes/configs/config.h").read_text().splitlines():
        fields = line.split()
        if len(fields) == 3 and fields[0] == "#define":
            macros[fields[1]] = fields[2]
    mismatches = {
        name: macros.get(name)
        for expected, names in (("0", DISABLED_MACROS), ("1", RETAINED_MACROS))
        for name in names
        if macros.get(name) != expected
    }
    if mismatches:
        raise RuntimeError(f"Unexpected generated capability macros: {mismatches}")
    print("Aiven hardened source exclusions and retained implementations passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("build_dir", type=Path)
    args = parser.parse_args()
    check(args.build_dir)