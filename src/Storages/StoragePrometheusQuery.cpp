#include <Storages/StoragePrometheusQuery.h>

#include <Common/logger_useful.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Setting
{
    extern const SettingsBool enable_materialized_cte;
}

namespace
{

/// Read a required String literal argument.
/// NOTE(aiven): upstream master reads this through `evaluateConstantExpressionAsColumn`, which returns
/// the new `Core/ConstantValue.h` type and avoids materializing a `Field`. Adding that entry point on
/// 26.3 means restructuring `evaluateConstantExpression` and de-duplicating `DB::ConstantValue`, which
/// still exists here as `Analyzer/ConstantValue.h` - six files of Analyzer-layer surgery for two call
/// sites that only read a string. So this uses 26.3's `Field`-based API, which is what upstream's own
/// comment describes as the previous behaviour: `operator[]` flattens `Nullable`/`LowCardinality`
/// wrappers, so a non-NULL wrapped String constant passes the type check, and NULL is still rejected.
String getStringConstArgument(const ASTPtr & arg, const ContextPtr & context, std::string_view arg_name)
{
    const auto [field, type] = evaluateConstantExpression(arg, context);
    if (!isStringOrFixedString(removeLowCardinalityAndNullable(type)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got {}", arg_name, type->getName());
    if (field.isNull())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument '{}' must be a literal with type String, got NULL", arg_name);
    return field.safeGet<String>();
}

}

StoragePrometheusQuery::Configuration StoragePrometheusQuery::getConfiguration(ASTs & args, const ContextPtr & context, bool over_range)
{
    std::string_view function_name = over_range ? "prometheusQueryRange" : "prometheusQuery";
    size_t min_num_args = 3 + over_range * 2;
    size_t max_num_args = 4 + over_range * 2;

    if ((args.size() < min_num_args) || (args.size() > max_num_args))
    {
        std::string_view expected_args = over_range ? "[database, ] time_series_table, promql_query, start_time, end_time, step"
                                                    : "[database, ] time_series_table, promql_query, evaluation_time";
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires {}..{} arguments: {}([database, ] time_series_table, promql_query, {})",
                        function_name, min_num_args, max_num_args, function_name, expected_args);
    }

    size_t argument_index = 0;

    StorageID time_series_storage_id = StorageID::createEmpty();

    if (args.size() == min_num_args)
    {
        /// prometheusQuery( [my_db.]my_time_series_table, ... )
        if (const auto * id = args[argument_index]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
            {
                time_series_storage_id = table_id->getTableId();
                ++argument_index;
            }
        }
    }

    if (time_series_storage_id.empty())
    {
        if (args.size() == min_num_args)
        {
            /// prometheusQuery( 'my_time_series_table', ... )
            time_series_storage_id.table_name = getStringConstArgument(args[argument_index++], context, "table_name");
        }
        else
        {
            /// prometheusQuery( 'mydb', 'my_time_series_table', ... )
            time_series_storage_id.database_name = getStringConstArgument(args[argument_index++], context, "database_name");
            time_series_storage_id.table_name = getStringConstArgument(args[argument_index++], context, "table_name");
        }
    }

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);

    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);
    UInt64 time_series_version = time_series_storage->getVersion();
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr();
    auto [timestamp_data_type, scalar_data_type] = splitTimeSeriesType(
        time_series_metadata->columns.get(TimeSeriesColumnNames::getOuterSamples(time_series_version)).type);

    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    PrometheusQueryTree promql_query{getStringConstArgument(args[argument_index++], context, "promql_query"), timestamp_scale};

    PrometheusQueryEvaluationMode mode = {};
    DateTime64 start_time;
    DateTime64 end_time;
    Decimal64 step;

    if (over_range)
    {
        auto [start_time_field, start_time_type] = evaluateConstantExpression(args[argument_index++], context);
        auto [end_time_field, end_time_type] = evaluateConstantExpression(args[argument_index++], context);
        auto [step_field, step_type] = evaluateConstantExpression(args[argument_index++], context);

        mode = PrometheusQueryEvaluationMode::QUERY_RANGE;
        start_time = parseTimeSeriesTimestamp(start_time_field, start_time_type, timestamp_scale);
        end_time = parseTimeSeriesTimestamp(end_time_field, end_time_type, timestamp_scale);
        step = parseTimeSeriesDuration(step_field, step_type, timestamp_scale);
    }
    else
    {
        auto [time_field, time_type] = evaluateConstantExpression(args[argument_index++], context);

        mode = PrometheusQueryEvaluationMode::QUERY;
        start_time = parseTimeSeriesTimestamp(time_field, time_type, timestamp_scale);
        end_time = start_time;
        step = 0;
    }

    chassert(argument_index == args.size());

    Configuration config;
    config.promql_query = std::make_shared<PrometheusQueryTree>(std::move(promql_query));
    auto & evaluation_settings = config.evaluation_settings;
    evaluation_settings.time_series_storage_id = std::move(time_series_storage_id);
    evaluation_settings.timestamp_data_type = std::move(timestamp_data_type);
    evaluation_settings.scalar_data_type = std::move(scalar_data_type);
    evaluation_settings.time_series_version = time_series_version;
    evaluation_settings.mode = mode;
    evaluation_settings.start_time = start_time;
    evaluation_settings.end_time = end_time;
    evaluation_settings.step = step;
    return config;
}

StoragePrometheusQuery::StoragePrometheusQuery(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const Configuration & config_)
    : StorageWithCommonVirtualColumns{table_id_}
    , config(config_)
    , log(getLogger("StoragePrometheusQuery"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    /// NOTE(aiven): upstream master moved virtuals into the in-memory metadata (upstream #102644), so it
    /// calls `storage_metadata.setVirtuals` and tags each ephemeral virtual with a
    /// `VirtualsMaterializationPlace`. On 26.3 virtuals hang off `IStorage` and that enum does not exist;
    /// `StorageWithCommonVirtualColumns` materializes `_database`/`_table` in the plan regardless.
    setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StoragePrometheusQuery::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "");
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "");
    return desc;
}

void StoragePrometheusQuery::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(config.evaluation_settings.time_series_storage_id, context));
    checkTimeSeriesVersionSupportedByPromQL(*time_series_storage);

    LOG_INFO(log, "Building SQL to evaluate promql: {}", *config.promql_query);
    PrometheusQueryToSQL::Converter converter{config.promql_query, config.evaluation_settings};
    ASTPtr select_query = converter.getSQL();

    LOG_INFO(log, "Will execute query:\n{}", select_query->formatForLogging());
    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    /// Isolate the settings required by generated PromQL from the outer query.
    auto query_context = Context::createCopy(context);
    if (!context->getSettingsRef()[Setting::enable_materialized_cte].changed)
        query_context->setSetting("enable_materialized_cte", true);
    query_context->setSetting("empty_result_for_aggregation_by_empty_set", false);

    InterpreterSelectQueryAnalyzer interpreter(select_query, query_context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
