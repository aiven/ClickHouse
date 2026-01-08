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

#if USE_ARROWFLIGHT
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
    registerTableFunctionTimeSeries(factory);

    registerTableFunctionObjectStorage(factory);
    registerTableFunctionObjectStorageCluster(factory);
    registerDataLakeTableFunctions(factory);
    registerDataLakeClusterTableFunctions(factory);

#if USE_YTSAURUS
    registerTableFunctionYTsaurus(factory);
#endif

}

}
