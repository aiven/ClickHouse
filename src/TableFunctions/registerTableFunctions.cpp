#include <TableFunctions/registerTableFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
void registerTableFunctions()
{
    auto & factory = TableFunctionFactory::instance();

    registerTableFunctionMerge(factory);
    registerTableFunctionRemote(factory);
    registerTableFunctionNumbers(factory);
    registerTableFunctionLoop(factory);
    registerTableFunctionGenerateSeries(factory);
    registerTableFunctionNull(factory);
    registerTableFunctionZeros(factory);
#if REGISTER_EXECUTABLE_FUNCTION
    registerTableFunctionExecutable(factory);
#endif
#if REGISTER_FILE_FUNCTION
    registerTableFunctionFile(factory);
    registerTableFunctionFileCluster(factory);
#endif
#if REGISTER_URL_FUNCTION
    registerTableFunctionURL(factory);
#endif
#if REGISTER_URL_CLUSTER_FUNCTION
    registerTableFunctionURLCluster(factory);
#endif
    registerTableFunctionValues(factory);
    registerTableFunctionInput(factory);
    registerTableFunctionGenerate(factory);
#if USE_MONGODB && REGISTER_MONGODB_FUNCTION
    registerTableFunctionMongoDB(factory);
#endif
#if REGISTER_REDIS_FUNCTION
    registerTableFunctionRedis(factory);
#endif

#if USE_ARROWFLIGHT && REGISTER_ARROWFLIGHT_TABLE_ENGINE
    registerTableFunctionArrowFlight(factory);
#endif

    registerTableFunctionMergeTreeIndex(factory);
    registerTableFunctionMergeTreeProjection(factory);
    registerTableFunctionFuzzQuery(factory);
#if USE_RAPIDJSON || USE_SIMDJSON
    registerTableFunctionFuzzJSON(factory);
#endif

#if USE_HIVE && REGISTER_HIVE_FUNCTION
    registerTableFunctionHive(factory);
#endif

#if REGISTER_ODBC_FUNCTION
    registerTableFunctionODBC(factory);
    registerTableFunctionJDBC(factory);
#endif

    registerTableFunctionView(factory);
    registerTableFunctionViewIfPermitted(factory);

#if USE_MYSQL
    registerTableFunctionMySQL(factory);
#endif

#if USE_LIBPQXX
    registerTableFunctionPostgreSQL(factory);
#endif

#if USE_SQLITE
    registerTableFunctionSQLite(factory);
#endif

    registerTableFunctionDictionary(factory);

    registerTableFunctionFormat(factory);
    registerTableFunctionExplain(factory);
#if REGISTER_TIMESERIES_FUNCTION
    registerTableFunctionTimeSeries(factory);
#endif

#if REGISTER_OBJECT_STORAGE_FUNCTION
    registerTableFunctionObjectStorage(factory);
    registerTableFunctionObjectStorageCluster(factory);
#endif
#if REGISTER_DATALAKE_FUNCTION
    registerDataLakeTableFunctions(factory);
    registerDataLakeClusterTableFunctions(factory);
#endif

#if USE_YTSAURUS && REGISTER_YTSAURUS_FUNCTION
    registerTableFunctionYTsaurus(factory);
#endif

}

}
