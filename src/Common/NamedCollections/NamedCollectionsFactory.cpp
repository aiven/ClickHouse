#include <format>
#include <Common/NamedCollections/NamedCollections_fwd.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>
#include <Core/Settings.h>
#include <base/sleep.h>
#include <Databases/IDatabase.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
    extern const int NAMED_COLLECTION_IS_IMMUTABLE;
}

NamedCollectionFactory & NamedCollectionFactory::instance()
{
    static NamedCollectionFactory instance;
    return instance;
}

NamedCollectionFactory::~NamedCollectionFactory()
{
    shutdown();
}

void NamedCollectionFactory::shutdown()
{
    shutdown_called = true;
    if (update_task)
        update_task->deactivate();
    metadata_storage.reset();
}

bool NamedCollectionFactory::exists(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    return exists(collection_name, lock);
}

NamedCollectionPtr NamedCollectionFactory::get(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    auto collection = tryGet(collection_name, lock);
    if (!collection)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "There is no named collection `{}`",
            collection_name);
    }
    return collection;
}

NamedCollectionPtr NamedCollectionFactory::tryGet(const std::string & collection_name) const
{
    std::lock_guard lock(mutex);
    return tryGet(collection_name, lock);
}

NamedCollectionsMap NamedCollectionFactory::getAll() const
{
    std::lock_guard lock(mutex);
    return loaded_named_collections;
}

bool NamedCollectionFactory::exists(const std::string & collection_name, std::lock_guard<std::mutex> &) const
{
    return loaded_named_collections.contains(collection_name);
}

MutableNamedCollectionPtr NamedCollectionFactory::tryGet(
    const std::string & collection_name,
    std::lock_guard<std::mutex> &) const
{
    auto it = loaded_named_collections.find(collection_name);
    if (it == loaded_named_collections.end())
        return nullptr;
    return it->second;
}

MutableNamedCollectionPtr NamedCollectionFactory::getMutable(
    const std::string & collection_name,
    std::lock_guard<std::mutex> & lock) const
{
    auto collection = tryGet(collection_name, lock);
    if (!collection)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "There is no named collection `{}`",
            collection_name);
    }
    else if (!collection->isMutable())
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_IS_IMMUTABLE,
            "Cannot get collection `{}` for modification, "
            "because collection was defined as immutable",
            collection_name);
    }
    return collection;
}

void NamedCollectionFactory::add(
    const std::string & collection_name,
    MutableNamedCollectionPtr collection,
    std::lock_guard<std::mutex> &)
{
    auto [it, inserted] = loaded_named_collections.emplace(collection_name, collection);
    if (!inserted)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
            "A named collection `{}` already exists",
            collection_name);
    }
}

void NamedCollectionFactory::add(NamedCollectionsMap collections, std::lock_guard<std::mutex> & lock)
{
    for (const auto & [collection_name, collection] : collections)
        add(collection_name, collection, lock);
}

void NamedCollectionFactory::remove(const std::string & collection_name, std::lock_guard<std::mutex> & lock, bool force)
{
    bool removed = removeIfExists(collection_name, lock, force);
    if (!removed)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "There is no named collection `{}`",
            collection_name);
    }
}

bool NamedCollectionFactory::removeIfExists(
    const std::string & collection_name,
    std::lock_guard<std::mutex> & lock,
    bool force)
{
    auto collection = tryGet(collection_name, lock);
    if (!collection)
        return false;

    if (!force && !collection->isMutable())
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_IS_IMMUTABLE,
            "Cannot get collection `{}` for modification, "
            "because collection was defined as immutable",
            collection_name);
    }
    loaded_named_collections.erase(collection_name);
    return true;
}

void NamedCollectionFactory::removeById(NamedCollection::SourceId id, std::lock_guard<std::mutex> &)
{
    std::erase_if(
        loaded_named_collections,
        [&](const auto & value) { return value.second->getSourceId() == id; });
}

namespace
{
    constexpr auto NAMED_COLLECTIONS_CONFIG_PREFIX = "named_collections";

    std::vector<std::string> listCollections(const Poco::Util::AbstractConfiguration & config)
    {
        Poco::Util::AbstractConfiguration::Keys collections_names;
        config.keys(NAMED_COLLECTIONS_CONFIG_PREFIX, collections_names);
        return collections_names;
    }

    MutableNamedCollectionPtr getCollection(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & collection_name)
    {
        const auto collection_prefix = fmt::format("{}.{}", NAMED_COLLECTIONS_CONFIG_PREFIX, collection_name);
        std::queue<std::string> enumerate_input;
        std::set<std::string, std::less<>> enumerate_result;

        enumerate_input.push(collection_prefix);
        NamedCollectionConfiguration::listKeys(config, std::move(enumerate_input), enumerate_result, -1);

        /// Collection does not have any keys. (`enumerate_result` == <collection_path>).
        const bool collection_is_empty = enumerate_result.size() == 1
            && *enumerate_result.begin() == collection_prefix;

        std::set<std::string, std::less<>> keys;
        if (!collection_is_empty)
        {
            /// Skip collection prefix and add +1 to avoid '.' in the beginning.
            for (const auto & path : enumerate_result)
                keys.emplace(path.substr(collection_prefix.size() + 1));
        }

        return NamedCollection::create(
            config, collection_name, collection_prefix, keys, NamedCollection::SourceId::CONFIG, /* is_mutable */false);
    }

    NamedCollectionsMap getNamedCollections(const Poco::Util::AbstractConfiguration & config)
    {
        NamedCollectionsMap result;
        for (const auto & collection_name : listCollections(config))
        {
            if (result.contains(collection_name))
            {
                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                    "Found duplicate named collection `{}`",
                    collection_name);
            }
            result.emplace(collection_name, getCollection(config, collection_name));
        }
        return result;
    }
}

void NamedCollectionFactory::loadIfNot()
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
}

bool NamedCollectionFactory::loadIfNot(std::lock_guard<std::mutex> & lock)
{
    if (loaded)
        return false;

    auto context = Context::getGlobalContextInstance();
    metadata_storage = NamedCollectionsMetadataStorage::create(context);

    loadFromConfig(context->getConfigRef(), lock);
    loadFromSQL(lock);

    if (metadata_storage->isReplicated())
    {
        update_task = context->getSchedulePool().createTask("NamedCollectionsMetadataStorage", [this]{ updateFunc(); });
        update_task->activate();
        update_task->schedule();
    }

    loaded = true;
    return true;
}

void NamedCollectionFactory::loadFromConfig(const Poco::Util::AbstractConfiguration & config, std::lock_guard<std::mutex> & lock)
{
    auto collections = getNamedCollections(config);
    LOG_TEST(log, "Loaded {} collections from config", collections.size());
    add(std::move(collections), lock);
}

void NamedCollectionFactory::reloadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto context = Context::getGlobalContextInstance();
    std::set<String> new_or_changed;
    std::set<String> removed;
    {
        std::lock_guard lock(mutex);
        if (loadIfNot(lock))
            return;

        auto collections = getNamedCollections(config);
        LOG_TEST(log, "Loaded {} collections from config", collections.size());

        // Add or update collections from the config
        for (const auto & [name, collection] : collections)
        {
            if (const auto existing_collection = tryGet(name, lock))
            {
                if (existing_collection->getSourceId() != NamedCollection::SourceId::CONFIG)
                {
                    LOG_ERROR(
                        &Poco::Logger::get("NamedCollectionsFactory"),
                        "Named collection {} from config conflicts with existing named collection",
                        name);
                }
                else if (*existing_collection != *collection)
                {
                    remove(name, lock, /* force */ true);
                    add(name, collection, lock);
                    new_or_changed.emplace(name);
                }
            }
            else
            {
                add(name, collection, lock);
                new_or_changed.emplace(name);
            }
        }

        // Remove collections that are no longer in the config
        for (const auto & [name, collection] : loaded_named_collections)
        {
            if (!collections.contains(name) && collection->getSourceId() == NamedCollection::SourceId::CONFIG)
                removed.emplace(name);
        }
        for (const auto &name : removed)
            remove(name, lock, /* force */ true);

        // If no changes were made, return early
        if (new_or_changed.empty() && removed.empty())
        {
            loaded = true;
            return;
        }

    }

    // Reload tables that use named collections that were changed or removed
    DatabaseCatalog & catalog = DatabaseCatalog::instance();
    Databases databases = catalog.getDatabases();
    for (const auto & [database_name, database]: databases)
    {
        if (!database->supportsNamedCollectionReloading())
            continue;

        for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const String & table_name = iterator->name();
            const StoragePtr & table = iterator->table();
            if (!table)
                continue;
            const std::optional<String> collection_name = table->getNamedCollectionName();
            if (!collection_name.has_value())
                continue;
            if (new_or_changed.contains(*collection_name))
            {
                ASTPtr query = database->getCreateTableQuery(table_name, context);
                auto create_query = query->as<ASTCreateQuery &>();
                const ASTFunction * engine_def = create_query.storage->engine;

                ASTs engine_args;
                if (engine_def->arguments)
                    engine_args = engine_def->arguments->children;

                try
                {
                    table->reload(context, engine_args);
                }
                catch (...)
                {
                    auto message = std::format("Failed to reload table {} with collection {}", table_name, *collection_name);
                    tryLogCurrentException(&Poco::Logger::get("NamedCollectionsFactory"), message);
                }
            }
            else if (removed.contains(*collection_name))
            {
                table->namedCollectionDeleted();
            }
        }
    }
    loaded = true;
}

void NamedCollectionFactory::loadFromSQL(std::lock_guard<std::mutex> & lock)
{
    auto collections = metadata_storage->getAll();
    LOG_TEST(log, "Loaded {} collections from sql", collections.size());
    add(std::move(collections), lock);
}

void NamedCollectionFactory::createFromSQL(const ASTCreateNamedCollectionQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (exists(query.collection_name, lock))
    {
        if (query.if_not_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
            "A named collection `{}` already exists",
            query.collection_name);
    }

    add(query.collection_name, metadata_storage->create(query), lock);
}

void NamedCollectionFactory::removeFromSQL(const ASTDropNamedCollectionQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (!exists(query.collection_name, lock))
    {
        if (query.if_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot remove collection `{}`, because it doesn't exist",
            query.collection_name);
    }

    metadata_storage->remove(query.collection_name);
    remove(query.collection_name, lock, /* force */ false);
}

void NamedCollectionFactory::updateFromSQL(const ASTAlterNamedCollectionQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (!exists(query.collection_name, lock))
    {
        if (query.if_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot remove collection `{}`, because it doesn't exist",
            query.collection_name);
    }

    metadata_storage->update(query);

    auto collection = getMutable(query.collection_name, lock);
    auto collection_lock = collection->lock();

    for (const auto & [name, value] : query.changes)
    {
        auto it_override = query.overridability.find(name);
        if (it_override != query.overridability.end())
            collection->setOrUpdate<String, true>(name, convertFieldToString(value), it_override->second);
        else
            collection->setOrUpdate<String, true>(name, convertFieldToString(value), {});
    }

    for (const auto & key : query.delete_keys)
        collection->remove<true>(key);
}

void NamedCollectionFactory::reloadFromSQL()
{
    std::lock_guard lock(mutex);
    if (loadIfNot(lock))
        return;

    auto collections = metadata_storage->getAll();
    removeById(NamedCollection::SourceId::SQL, lock);
    add(std::move(collections), lock);
}

bool NamedCollectionFactory::usesReplicatedStorage()
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return metadata_storage->isReplicated();
}

void NamedCollectionFactory::updateFunc()
{
    LOG_TRACE(log, "Named collections background updating thread started");

    while (!shutdown_called.load())
    {
        if (metadata_storage->waitUpdate())
        {
            try
            {
                reloadFromSQL();
            }
            catch (const Coordination::Exception & e)
            {
                if (Coordination::isHardwareError(e.code))
                {
                    LOG_INFO(log, "Lost ZooKeeper connection, will try to connect again: {}",
                            DB::getCurrentExceptionMessage(true));

                    sleepForSeconds(1);
                }
                else
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                    chassert(false);
                }
                continue;
            }
            catch (...)
            {
                DB::tryLogCurrentException(__PRETTY_FUNCTION__);
                chassert(false);
                continue;
            }
        }
    }

    LOG_TRACE(log, "Named collections background updating thread finished");
}

}
