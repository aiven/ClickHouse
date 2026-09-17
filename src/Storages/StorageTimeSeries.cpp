#include <Storages/StorageTimeSeries.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Processors/QueryPlan/ReadFromTimeSeries.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/IBackup.h>
#include <Backups/RestorerFromBackup.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/TimeSeries/TimeSeriesSink.h>
#include <Parsers/getTimeSeriesSettingVersion.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>
#include <Storages/TimeSeries/makeASTSelectFromTimeSeries.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>
#include <base/insertAtEnd.h>
#include <filesystem>
#include <boost/algorithm/string.hpp>
#include <base/EnumReflection.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_time_series_table;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsUInt64 version;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNEXPECTED_TABLE_ENGINE;
    extern const int UNKNOWN_TABLE;
}

namespace fs = std::filesystem;


namespace
{
    /// Normalizes the create query.
    boost::intrusive_ptr<const ASTCreateQuery> makeNormalizedCreateQuery(
        const ASTCreateQuery & query, const ContextPtr & local_context, LoadingStrictnessLevel mode, bool is_restore_from_backup)
    {
        auto copy = boost::static_pointer_cast<ASTCreateQuery>(query.clone());
        normalizeTimeSeriesDefinition(*copy, local_context, mode, is_restore_from_backup);
        return copy;
    }

    /// We allow altering only two settings: `id_generator` and `filter_by_min_time_and_max_time`.
    void checkSettingCanBeAltered(std::string_view setting_name, std::string_view storage_name)
    {
        if ((setting_name != "id_generator") && (setting_name != "filter_by_min_time_and_max_time"))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Setting '{}' of storage {} cannot be changed after the table is created", setting_name, storage_name);
    }
}


std::vector<StorageTimeSeries::Target> StorageTimeSeries::findTargets(const ASTCreateQuery & create_query)
{
    std::vector<Target> targets;
    for (auto target_kind : getTargetKinds())
    {
        /// The recent samples target exists only if the normalized create query has a RECENT SAMPLES clause.
        /// The `recent_samples_ttl_seconds` setting itself cannot be checked here instead: a table created
        /// before this feature existed has no recent samples table on disk while the setting reads as its
        /// non-zero default, and ATTACH never creates inner tables.
        if ((target_kind == ViewTarget::RecentSamples)
            && (!create_query.targets || !create_query.targets->tryGetTarget(target_kind)))
            continue;

        Target target;
        target.kind = target_kind;

        if (auto target_table_id = create_query.getTargetTableID(target_kind))
        {
            /// A target table is specified.
            target.table_id = target_table_id;
        }
        else
        {
            /// An inner target table should be used.
            target.table_id.uuid = create_query.getTargetInnerUUID(target_kind);
            target.is_inner_table = true;
        }

        targets.emplace_back(std::move(target));
    }

    return targets;
}


std::vector<StorageTimeSeries::Target> StorageTimeSeries::buildTargets(
    const ASTCreateQuery & create_query,
    const StorageID & table_id,
    const ContextPtr & local_context,
    LoadingStrictnessLevel mode)
{
    if (mode <= LoadingStrictnessLevel::CREATE && !local_context->getSettingsRef()[Setting::enable_time_series_table])
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "TimeSeries table engine "
                        "is not enabled (the setting 'enable_time_series_table')");
    }

    auto targets = findTargets(create_query);

    /// Inner target tables are created only for a new table, ATTACH expects them to exist already.
    if (mode <= LoadingStrictnessLevel::SECONDARY_CREATE)
    {
        for (const auto & target : targets)
        {
            if (!target.is_inner_table)
                continue;

            /// Create the inner target table using the pre-computed inner columns from the create query.
            /// The normalization always sets them; a query with an inner UUID but no inner columns
            /// can only come from hand-edited metadata.
            auto * inner_columns = create_query.getTargetInnerColumns(target.kind);
            if (!inner_columns)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} target of table {} has no inner columns",
                    magic_enum::enum_name(target.kind), table_id.getNameForLogs());
            auto inner_engine = boost::static_pointer_cast<ASTStorage>(
                create_query.getTargetInnerEngine(target.kind)
                    ? create_query.getTargetInnerEngine(target.kind)->ptr()
                    : ASTPtr{});
            createTimeSeriesInnerTable(
                target.kind, target.table_id.uuid, *inner_columns, inner_engine, table_id, getTimeSeriesSettingVersion(create_query), local_context);
        }
    }

    return targets;
}


StorageTimeSeries::StorageTimeSeries(
    const StorageID & table_id,
    const ContextPtr & local_context,
    LoadingStrictnessLevel mode,
    bool is_restore_from_backup,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns,
    const String & comment)
    : StorageWithCommonVirtualColumns(table_id)
    , WithContext(local_context->getGlobalContext())
{
    StorageInMemoryMetadata storage_metadata;
    auto settings = std::make_unique<TimeSeriesSettings>();

    /// The version is checked before the normalization: a definition of an unsupported version can't be normalized
    /// because it may contain settings or columns unknown to this server.
    UInt64 version = getTimeSeriesSettingVersion(query);

    /// A table with an unsupported version can appear after a downgrade of ClickHouse. It's attached anyway,
    /// so that it can be inspected with SHOW CREATE TABLE and dropped together with its inner tables,
    /// while every operation over it is rejected (see TimeSeriesVersion.h).
    /// A new table with an unsupported version is rejected by `checkTimeSeriesSettings` during the normalization below.
    bool is_version_supported = isTimeSeriesVersionSupported(version);

    /// An unsupported version is tolerated only for ATTACH.
    if (!is_version_supported && (mode >= LoadingStrictnessLevel::ATTACH))
    {
        /// Only the identifiers of the target tables are taken from the definition.
        targets = findTargets(query);
        (*settings)[TimeSeriesSetting::version] = version;
        storage_metadata.setColumns(columns);
    }
    else
    {
        auto normalized_create_query = makeNormalizedCreateQuery(query, local_context, mode, is_restore_from_backup);
        targets = buildTargets(*normalized_create_query, table_id, local_context, mode);

        /// Load TimeSeries settings from the `SETTINGS` clause.
        if (normalized_create_query->storage)
            settings->loadFromQuery(*normalized_create_query->storage);

        /// Re-derive columns from the normalized AST rather than trusting the `columns` argument.
        /// For CREATE / RESTORE the query arrives already normalized.
        /// However for ATTACH InterpreterCreateQuery doesn't normalize the create query,
        /// so `columns` can contain prealpha outer columns which we should upgrade.
        auto normalized_columns = InterpreterCreateQuery::getColumnsDescription(
            *normalized_create_query->columns_list->columns, local_context, mode);
        storage_metadata.setColumns(normalized_columns);

        /// The metadata must carry the whole `SETTINGS` clause because a settings alter changes that clause
        /// and the result replaces the one in the create query.
        if (normalized_create_query->storage && normalized_create_query->storage->settings)
            storage_metadata.setSettingsChanges(normalized_create_query->storage->settings->clone());
    }

    has_inner_tables = std::ranges::any_of(targets, &Target::is_inner_table);
    storage_settings.set(std::move(settings));

    if (!comment.empty())
        storage_metadata.setComment(comment);
    /// NOTE(aiven): upstream master moved virtuals into the in-memory metadata (upstream #102644), so it
    /// calls `storage_metadata.setVirtuals` and tags each ephemeral virtual with a
    /// `VirtualsMaterializationPlace`. On 26.3 virtuals hang off `IStorage` and that enum does not exist;
    /// `StorageWithCommonVirtualColumns` materializes `_database`/`_table` in the plan regardless.
    setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}


UInt64 StorageTimeSeries::getVersion() const
{
    return (*storage_settings.get())[TimeSeriesSetting::version];
}


StorageTimeSeries::~StorageTimeSeries() = default;


const StorageTimeSeries::Target * StorageTimeSeries::tryGetTarget(ViewTarget::Kind target_kind) const
{
    for (const auto & target : targets)
    {
        if (target.kind == target_kind)
            return &target;
    }
    return nullptr;
}


bool StorageTimeSeries::hasTarget(ViewTarget::Kind target_kind) const
{
    return tryGetTarget(target_kind) != nullptr;
}


StoragePtr StorageTimeSeries::getTargetTable(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    return getTargetTableImpl(target_kind, local_context, /* throw_if_not_found = */ true);
}

StoragePtr StorageTimeSeries::tryGetTargetTable(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    return getTargetTableImpl(target_kind, local_context, /* throw_if_not_found = */ false);
}

StoragePtr StorageTimeSeries::getTargetTableImpl(ViewTarget::Kind target_kind, const ContextPtr & local_context, bool throw_if_not_found) const
{
    const auto * target_ptr = tryGetTarget(target_kind);
    if (!target_ptr)
    {
        /// The recent samples target is optional.
        if (target_kind == ViewTarget::RecentSamples)
        {
            if (throw_if_not_found)
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries table {} has no {} target table",
                                getStorageID().getNameForLogs(), target_kind);
            return nullptr;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected target kind {}", target_kind);
    }
    const auto & target = *target_ptr;

    auto lookup = [&](const StorageID & id) -> StoragePtr
    {
        return DatabaseCatalog::instance()
            .tryGetDatabaseAndTable(local_context->tryResolveStorageID(id), local_context)
            .second;
    };

    /// For external targets `target.table_id` contains a table name.
    if (!target.table_id.table_name.empty())
    {
        auto res = lookup(target.table_id);
        if (!res && throw_if_not_found)
        {
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "The {} target table {} for TimeSeries table {} doesn't exist",
                            target_kind, target.table_id.getNameForLogs(), getStorageID().getNameForLogs());
        }
        return res;
    }

    /// For inner targets in Atomic databases `target.table_id` has a UUID but no name — look up directly by UUID.
    if (target.table_id.hasUUID())
    {
        auto res = DatabaseCatalog::instance().tryGetByUUID(target.table_id.uuid).second;
        if (!res && throw_if_not_found)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "The {} inner table {} for TimeSeries table {} doesn't exist",
                            target_kind, target.table_id.getNameForLogs(), getStorageID().getNameForLogs());
        return res;
    }

    chassert(target.table_id.empty());

    /// For inner targets in non-Atomic databases, `target.table_id` is empty and we look up the inner table by its constructed name.
    StorageID time_series_table_id = getStorageID();
    StorageID inner_table_id{time_series_table_id.getDatabaseName(), getTimeSeriesInnerTableName(target_kind, time_series_table_id, getVersion())};

    if (auto res = lookup(inner_table_id))
        return res;

    /// Fallback for legacy tables created before the samples inner table was renamed
    /// from `.inner.data.*` to `.inner.samples.*`
    if (target_kind == ViewTarget::Samples)
    {
        inner_table_id.table_name = getTimeSeriesInnerTableName("data", time_series_table_id);
        if (auto res = lookup(inner_table_id))
            return res;
    }

    if (throw_if_not_found)
    {
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "The {} inner table {} for TimeSeries table {} doesn't exist",
                        target_kind, inner_table_id.getNameForLogs(), getStorageID().getNameForLogs());
    }

    return nullptr;
}


StorageID StorageTimeSeries::getTargetTableID(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    return getTargetTable(target_kind, local_context)->getStorageID();
}

StorageID StorageTimeSeries::tryGetTargetTableID(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    if (auto target_table = tryGetTargetTable(target_kind, local_context))
        return target_table->getStorageID();
    return StorageID::createEmpty();
}

bool StorageTimeSeries::isInnerTable(ViewTarget::Kind target_kind) const
{
    const auto * target = tryGetTarget(target_kind);
    if (!target)
    {
        /// The recent samples target is optional.
        if (target_kind == ViewTarget::RecentSamples)
            return false;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected target kind {}", target_kind);
    }
    return target->is_inner_table;
}


void StorageTimeSeries::drop()
{
    /// Sync flag and the setting make sense for Atomic databases only.
    /// However, with Atomic databases, IStorage::drop() can be called only from a background task in DatabaseCatalog.
    /// Running synchronous DROP from that task leads to deadlock.
    dropInnerTableIfAny(/* sync= */ false, getContext());
}

void StorageTimeSeries::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    if (!hasInnerTables())
        return;

    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            if (auto inner_table_id = tryGetTargetTableID(target_kind, local_context))
            {
                /// DDLGuards must be locked in order of increasing table name, so the inner guard
                /// may be requested only when this table's name sorts first.
                bool may_lock_ddl_guard = getStorageID().getQualifiedName() < inner_table_id.getQualifiedName();
                InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, inner_table_id,
                                                    sync, /* ignore_sync_setting= */ true, may_lock_ddl_guard);
            }
        }
    }
}

void StorageTimeSeries::checkTableCanBeDropped(ContextPtr query_context) const
{
    if (!hasInnerTables())
        return;

    for (auto target_kind : getTargetKinds())
    {
        if (!isInnerTable(target_kind))
            continue;

        if (auto inner_table = tryGetTargetTable(target_kind, query_context))
            inner_table->checkTableCanBeDropped(query_context);
    }
}

void StorageTimeSeries::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    checkTimeSeriesVersionIsSupported(*this);

    if (!hasInnerTables())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TimeSeries table {} targets only existing tables. Execute the statement directly on it.",
                        getStorageID().getNameForLogs());
    }

    for (auto target_kind : getTargetKinds())
    {
        /// We truncate only inner tables here.
        if (isInnerTable(target_kind))
        {
            auto inner_table_id = getTargetTableID(target_kind, local_context);
            /// NOTE(aiven): upstream master passes an eighth `propagate_metadata_transaction= false` argument
            /// here (the transaction can only be consumed once, so the DDL worker commits it after all the inner
            /// tables are truncated). On 26.3 `InterpreterDropQuery::executeDropQuery` takes seven parameters and
            /// has no such concept, so the argument is dropped.
            InterpreterDropQuery::executeDropQuery(
                ASTDropQuery::Kind::Truncate, getContext(), local_context, inner_table_id, /* sync= */ true,
                /* ignore_sync_setting= */ false, /* need_ddl_guard= */ false);
        }
    }
}


/// TODO: Return the row count of the inner "tags" table instead of the sum over all the inner tables:
/// it matches `SELECT count()` without FINAL, allowing the trivial count optimization.
std::optional<UInt64> StorageTimeSeries::totalRows(ContextPtr query_context) const
{
    if (!hasInnerTables())
        return 0;
    UInt64 total_rows = 0;
    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            auto inner_table = tryGetTargetTable(target_kind, query_context);
            if (!inner_table)
                return std::nullopt;

            auto total_rows_in_inner_table = inner_table->totalRows(query_context);
            if (!total_rows_in_inner_table)
                return std::nullopt;

            total_rows += *total_rows_in_inner_table;
        }
    }
    return total_rows;
}

std::optional<UInt64> StorageTimeSeries::totalBytes(ContextPtr query_context) const
{
    if (!hasInnerTables())
        return 0;
    UInt64 total_bytes = 0;
    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            auto inner_table = tryGetTargetTable(target_kind, query_context);
            if (!inner_table)
                return std::nullopt;

            auto total_bytes_in_inner_table = inner_table->totalBytes(query_context);
            if (!total_bytes_in_inner_table)
                return std::nullopt;

            total_bytes += *total_bytes_in_inner_table;
        }
    }
    return total_bytes;
}

std::optional<UInt64> StorageTimeSeries::totalBytesUncompressed(const Settings & settings) const
{
    if (!hasInnerTables())
        return 0;
    UInt64 total_bytes = 0;
    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            auto inner_table = tryGetTargetTable(target_kind, getContext());
            if (!inner_table)
                return std::nullopt;

            auto total_bytes_in_inner_table = inner_table->totalBytesUncompressed(settings);
            if (!total_bytes_in_inner_table)
                return std::nullopt;

            total_bytes += *total_bytes_in_inner_table;
        }
    }
    return total_bytes;
}

Strings StorageTimeSeries::getDataPaths() const
{
    Strings data_paths;
    for (auto target_kind : getTargetKinds())
    {
        auto table = tryGetTargetTable(target_kind, getContext());
        if (!table)
            continue;

        insertAtEnd(data_paths, table->getDataPaths());
    }
    return data_paths;
}


bool StorageTimeSeries::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr &,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr local_context)
{
    checkTimeSeriesVersionIsSupported(*this);

    if (!hasInnerTables())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TimeSeries table {} targets only existing tables. Execute the statement directly on it.",
                        getStorageID().getNameForLogs());
    }

    bool optimized = false;
    for (auto target_kind : getTargetKinds())
    {
        if (isInnerTable(target_kind))
        {
            auto inner_table = getTargetTable(target_kind, local_context);
            const auto inner_metadata = inner_table->getInMemoryMetadataPtr();
            optimized |= inner_table->optimize(query, inner_metadata, partition, final, deduplicate, deduplicate_by_columns, cleanup, local_context);
        }
    }

    return optimized;
}


void StorageTimeSeries::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    /// A server must not alter the definition of a table with a version it doesn't support:
    /// it would rewrite the stored definition according to its own schema.
    checkTimeSeriesVersionIsSupported(*this);

    for (const auto & command : commands)
    {
        if (command.isCommentAlter() || command.type == AlterCommand::MODIFY_SQL_SECURITY)
            continue;
        if (command.type == AlterCommand::MODIFY_SETTING)
        {
            for (const auto & change : command.settings_changes)
                checkSettingCanBeAltered(change.name, getName());
            continue;
        }
        if (command.type == AlterCommand::RESET_SETTING)
        {
            for (const auto & name : command.settings_resets)
                checkSettingCanBeAltered(name, getName());
            continue;
        }
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

void StorageTimeSeries::alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder &)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    StorageInMemoryMetadata new_metadata = *metadata_snapshot;
    params.apply(new_metadata, local_context);

    std::unique_ptr<TimeSeriesSettings> new_settings;

    bool has_settings_changes = std::any_of(
        params.begin(), params.end(), [](const AlterCommand & c) { return c.isSettingsAlter(); });

    if (has_settings_changes)
    {
        chassert(new_metadata.settings_changes);
        /// Round-trip through `TimeSeriesSettings` to validate the names/values and
        /// to write them back in a canonical form.
        new_settings = std::make_unique<TimeSeriesSettings>();
        new_settings->applyChanges(new_metadata.settings_changes->as<const ASTSetQuery &>().changes);
        checkTimeSeriesSettings(*new_settings);
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->is_standalone = false;
        settings_ast->changes = new_settings->changes();
        new_metadata.settings_changes = settings_ast;
    }

    auto time_series_table_id = getStorageID();
    DatabaseCatalog::instance().getDatabase(time_series_table_id.database_name)->alterTable(
        local_context, time_series_table_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);

    if (new_settings)
        storage_settings.set(std::move(new_settings));
}


void StorageTimeSeries::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();

    /// In an Atomic/Replicated database both ids carry a UUID; inner tables are addressed by the
    /// outer table's UUID (the `.inner_id.<kind>.<uuid>` name), which is preserved by the rename, so
    /// only the outer table id changes. In an Ordinary database the inner table names embed the old
    /// outer table name, so each inner table has to be renamed too (same as StorageMaterializedView).
    bool from_atomic_to_atomic_database = old_table_id.hasUUID() && new_table_id.hasUUID();

    if (!from_atomic_to_atomic_database && hasInnerTables())
    {
        /// Collect every inner table rename first so that all destination names can be checked
        /// before any rename is executed. The inner renames are not transactional, so renaming
        /// them one by one would leave the table half-renamed (some inner tables moved, the rest
        /// not) if a later destination name happened to be occupied.
        std::vector<std::pair<StorageID, String>> inner_renames;
        for (auto target_kind : getTargetKinds())
        {
            if (!isInnerTable(target_kind))
                continue;

            auto inner_table = tryGetTargetTable(target_kind, getContext());
            if (!inner_table)
                continue;

            auto inner_table_id = inner_table->getStorageID();
            auto new_inner_table_name = getTimeSeriesInnerTableName(target_kind, new_table_id, getVersion());

            if (DatabaseCatalog::instance().isTableExist(StorageID{new_table_id.database_name, new_inner_table_name}, getContext()))
                throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {} already exists",
                                StorageID{new_table_id.database_name, new_inner_table_name}.getNameForLogs());

            inner_renames.emplace_back(std::move(inner_table_id), std::move(new_inner_table_name));
        }

        for (const auto & [inner_table_id, new_inner_table_name] : inner_renames)
        {
            auto rename = make_intrusive<ASTRenameQuery>();
            rename->addElement(inner_table_id.database_name, inner_table_id.table_name,
                               new_table_id.database_name, new_inner_table_name);
            InterpreterRenameQuery(rename, getContext()).execute();
        }
    }

    IStorage::renameInMemory(new_table_id);
}


void StorageTimeSeries::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    checkTimeSeriesVersionIsSupported(*this);

    if (!hasInnerTables())
        return;

    for (auto target_kind : getTargetKinds())
    {
        /// We backup the target table's data only if it's inner.
        if (isInnerTable(target_kind))
        {
            auto table = getTargetTable(target_kind, backup_entries_collector.getContext());
            String kind_str{magic_enum::enum_name(target_kind)};
            boost::algorithm::to_lower(kind_str);
            /// A table of an older version keeps the folder name "metrics", so an older server can restore the backup.
            if (target_kind == ViewTarget::MetricFamilies && getVersion() < TimeSeriesVersion::MIN_WITH_METRIC_FAMILIES_TARGET_NAME)
                kind_str = "metrics";
            table->backupData(backup_entries_collector, fs::path{data_path_in_backup} / kind_str, {});
        }
    }
}

void StorageTimeSeries::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    checkTimeSeriesVersionIsSupported(*this);

    if (!hasInnerTables())
        return;

    for (auto target_kind : getTargetKinds())
    {
        /// We restore the target table's data only if it's inner.
        if (isInnerTable(target_kind))
        {
            auto table = getTargetTable(target_kind, restorer.getContext());
            String kind_str{magic_enum::enum_name(target_kind)};
            boost::algorithm::to_lower(kind_str);
            String target_data_path = fs::path{data_path_in_backup} / kind_str;
            /// Support legacy backups where the samples folder was named "data" instead of "samples".
            if (target_kind == ViewTarget::Samples && !restorer.getBackup()->hasFiles(target_data_path))
                target_data_path = fs::path{data_path_in_backup} / "data";
            /// Support backups where the metric families folder was named "metrics" instead of "metricfamilies".
            if (target_kind == ViewTarget::MetricFamilies && !restorer.getBackup()->hasFiles(target_data_path))
                target_data_path = fs::path{data_path_in_backup} / "metrics";
            table->restoreDataFromBackup(restorer, target_data_path, {});
        }
    }
}

VirtualColumnsDescription StorageTimeSeries::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "");
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "");
    return desc;
}

void StorageTimeSeries::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    checkTimeSeriesVersionIsSupported(*this);

    /// Run the generated read query on a child context with a few settings pinned so its results
    /// don't depend on the caller's session/profile (see getSettingsForSelectFromTimeSeries).
    auto read_context = Context::createCopy(local_context);
    read_context->applySettingsChanges(getSettingsForSelectFromTimeSeries());

    NameSet requested_columns{column_names.begin(), column_names.end()};
    auto select_query = makeASTSelectFromTimeSeries(*this, requested_columns, query_info, read_context);
    auto options = SelectQueryOptions(QueryProcessingStage::Complete, /* subquery_depth_ = */ 0, /* is_subquery_ = */ false,
                                      query_info.settings_limit_offset_done);
    InterpreterSelectQueryAnalyzer interpreter(select_query, read_context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();

    if (!query_plan.isInitialized())
        return;

    auto generated_plan = std::make_unique<QueryPlan>(std::move(query_plan));
    query_plan = QueryPlan();
    query_plan.addStep(std::make_unique<ReadFromTimeSeriesStep>(std::move(generated_plan), read_context));
}


SinkToStoragePtr StorageTimeSeries::write(
    const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool async_insert)
{
    checkTimeSeriesVersionIsWritable(*this);

    Names insert_columns;
    if (const auto * insert_query = query->as<ASTInsertQuery>())
    {
        if (insert_query->columns)
            for (const auto & col : insert_query->columns->children)
                insert_columns.push_back(col->getColumnName());
    }
    return std::make_shared<TimeSeriesSink>(*this, metadata_snapshot->getSampleBlock(), insert_columns, local_context, async_insert);
}


std::shared_ptr<StorageTimeSeries> storagePtrToTimeSeries(StoragePtr storage)
{
    if (auto res = typeid_cast<std::shared_ptr<StorageTimeSeries>>(storage))
        return res;

    throw Exception(
        ErrorCodes::UNEXPECTED_TABLE_ENGINE,
        "This operation can be executed on a TimeSeries table only, the engine of table {} is not TimeSeries",
        storage->getStorageID().getNameForLogs());
}

std::shared_ptr<const StorageTimeSeries> storagePtrToTimeSeries(ConstStoragePtr storage)
{
    if (auto res = typeid_cast<std::shared_ptr<const StorageTimeSeries>>(storage))
        return res;

    throw Exception(
        ErrorCodes::UNEXPECTED_TABLE_ENGINE,
        "This operation can be executed on a TimeSeries table only, the engine of table {} is not TimeSeries",
        storage->getStorageID().getNameForLogs());
}


void registerStorageTimeSeries(StorageFactory & factory);
void registerStorageTimeSeries(StorageFactory & factory)
{
    factory.registerStorage("TimeSeries", [](const StorageFactory::Arguments & args)
    {
        /// Pass local_context here to convey setting to inner tables.
        return std::make_shared<StorageTimeSeries>(
            args.table_id, args.getLocalContext(), args.mode, args.is_restore_from_backup,
            args.query, args.columns, args.comment);
    }
    ,
    {
        .supports_settings = true,
        .supports_schema_inference = true,
        .has_builtin_setting_fn = TimeSeriesSettings::hasBuiltin,
    }
    /// NOTE(aiven): upstream master passes a third `Documentation{...}` argument here, carrying the
    /// engine's reference documentation embedded in the binary. On 26.3 `StorageFactory::registerStorage`
    /// takes only (name, creator_fn, features), so the ~550-line docs blob is dropped. The docs live in
    /// `docs/` on this branch instead; adding the embedded-docs parameter is a separate upstream change.
    );
}

}
