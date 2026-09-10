#include <Interpreters/Access/InterpreterDropAccessEntityQuery.h>
#include <Interpreters/InterpreterFactory.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Access/MaskingPolicy.h>
#include <Access/DefinerDependencies.h>
#include <Interpreters/Context.h>
#include <base/range.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <IO/WriteHelpers.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Storages/IStorage.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int HAVE_DEPENDENT_OBJECTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int ACCESS_DENIED;
}


BlockIO InterpreterDropAccessEntityQuery::execute()
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query_ptr->as<ASTDropAccessEntityQuery &>();

    /// Masking policies are available only in ClickHouse Cloud. Reject `DROP MASKING POLICY` outright in
    /// open-source builds (including the `IF EXISTS` and `ON CLUSTER` forms), consistently with `CREATE`,
    /// `ALTER` and `SHOW CREATE MASKING POLICY`, instead of silently no-op'ing (`IF EXISTS`) or reporting
    /// a confusing `UNKNOWN_MASKING_POLICY` error from the always-empty open-source access storage.
    if (query.type == AccessEntityType::MASKING_POLICY)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Masking Policies are available only in ClickHouse Cloud");

    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(getRequiredAccess());

    auto access = getContext()->getAccess();
    const String current_user_name = getContext()->getUserName();

    auto check_func = [access, current_user_name](const AccessEntityPtr & entity)
    {
        /// Self-protection is unconditional: PROTECTED_ACCESS_MANAGEMENT does not buy the right
        /// to drop the identity you are acting as, which would leave the session orphaned.
        if (entity->getType() == AccessEntityType::USER && entity->getName() == current_user_name)
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "User `{}` cannot drop themselves, even with PROTECTED_ACCESS_MANAGEMENT permission",
                current_user_name);

        if (entity->isProtected())
            access->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
    };

    Strings names_to_check;
    if (query.type == AccessEntityType::ROW_POLICY)
    {
        /// Qualify the database for the *check* only. The AST that goes ON CLUSTER must keep the
        /// unqualified form, because each replica resolves it against its own current database -
        /// that is why upstream calls `replaceEmptyDatabase` only on the local path below.
        auto names_for_check = boost::static_pointer_cast<ASTRowPolicyNames>(query.row_policy_names->clone());
        names_for_check->replaceEmptyDatabase(getContext()->getCurrentDatabase());
        names_to_check = names_for_check->toStrings();
    }
    else if (query.type == AccessEntityType::MASKING_POLICY)
        names_to_check = Strings{query.masking_policy_name->toString()};
    else
        names_to_check = query.names;

    /// Aiven patch 022. Enforce self-protection and the protected-entity policy on the initiator
    /// *before* the ON CLUSTER dispatch below, so the check cannot be laundered through
    /// `DDLWorker`. Targets are resolved against the whole access control rather than
    /// `query.storage_name`, which need not resolve on the initiator.
    const auto target_ids = access_control.find(query.type, names_to_check);
    for (const auto & id : target_ids)
    {
        if (auto entity = access_control.tryRead(id))
            check_func(entity);
    }

    /// Aiven patch 022, new on the 26.8 line: the dependency-cascade guard.
    ///
    /// `IAccessStorage::remove` calls `removeReferencesToRemovedIDs` after a successful removal,
    /// which rewrites every entity that referenced the dropped id through `updateImpl`. That path
    /// carries no `CheckFunc` and runs with no user identity in scope, so it can strip default
    /// roles, granted roles, grantees or settings from a protected entity behind both the
    /// interpreter checks and the storage-level check. Rather than weaken the cascade - which
    /// exists to prevent dangling references - we refuse the *triggering* DROP.
    ///
    /// The guard is deliberately narrow: it only fires when a protected entity actually depends
    /// on a target. A holder of the privilege is unaffected and the cascade runs as upstream
    /// intends. Exceptions from the scan propagate, so an unreadable entity refuses the drop
    /// rather than silently allowing it.
    if (!target_ids.empty() && !access->isGranted(AccessType::PROTECTED_ACCESS_MANAGEMENT))
    {
        const std::unordered_set<UUID> removed_ids(target_ids.begin(), target_ids.end());

        for (auto dependent_type : collections::range(AccessEntityType::MAX))
        {
            for (const auto & dependent_id : access_control.findAll(dependent_type))
            {
                if (removed_ids.contains(dependent_id))
                    continue;

                auto dependent = access_control.tryRead(dependent_id);
                if (!dependent || !dependent->isProtected())
                    continue;
                if (!dependent->hasDependencies(removed_ids))
                    continue;

                /// Always throws here: we already know the privilege is missing.
                access->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
            }
        }
    }

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    /// `check_func` is still passed down to the storage on the local path, as defense in depth:
    /// it re-runs against the entity actually being removed, under the storage's own lock.
    auto do_drop = [&](const Strings & names, const String & storage_name)
    {
        IAccessStorage * storage = &access_control;
        MultipleAccessStorage::StoragePtr storage_ptr;
        if (!storage_name.empty())
        {
            storage_ptr = access_control.getStorageByName(storage_name);
            storage = storage_ptr.get();
        }

        if (query.if_exists)
            storage->remove(storage->find(query.type, names), /* throw_if_not_exists = */ false, check_func);
        else
            storage->remove(storage->getIDs(query.type, names), /* throw_if_not_exists = */ true, check_func);
    };

    if (query.type == AccessEntityType::USER)
    {
        auto & definer_dependencies = DefinerDependencies::instance();
        for (const auto & name : query.names)
        {
            std::vector<String> objects;
            for (const auto & uuid : definer_dependencies.getObjectsForDefiner(name))
            {
                auto & catalog = DatabaseCatalog::instance();
                if (const auto table = catalog.tryGetByUUID(uuid).second)
                    objects.push_back(table->getStorageID().getNameForLogs());
                else if (catalog.hasUUIDMapping(uuid))
                    /// A detached table.
                    objects.push_back(toString(uuid));
                /// Otherwise the object is gone and the dependency is stale.
            }
            if (!objects.empty())
                throw Exception(ErrorCodes::HAVE_DEPENDENT_OBJECTS, "User `{}` is used as a definer of {}.", name, toString(objects));
        }
    }

    if (query.type == AccessEntityType::ROW_POLICY)
        do_drop(query.row_policy_names->toStrings(), query.storage_name);
    else if (query.type == AccessEntityType::MASKING_POLICY)
        do_drop(Strings{query.masking_policy_name->toString()}, query.storage_name);
    else
        do_drop(query.names, query.storage_name);

    return {};
}


AccessRightsElements InterpreterDropAccessEntityQuery::getRequiredAccess() const
{
    const auto & query = query_ptr->as<const ASTDropAccessEntityQuery &>();
    AccessRightsElements res;
    switch (query.type)
    {
        case AccessEntityType::USER:
        {
            for (const auto & name : query.names)
                res.emplace_back(AccessType::DROP_USER, name);
            return res;
        }
        case AccessEntityType::ROLE:
        {
            for (const auto & name : query.names)
                res.emplace_back(AccessType::DROP_ROLE, name);
            return res;
        }
        case AccessEntityType::SETTINGS_PROFILE:
        {
            res.emplace_back(AccessType::DROP_SETTINGS_PROFILE);
            return res;
        }
        case AccessEntityType::ROW_POLICY:
        {
            if (query.row_policy_names)
            {
                for (const auto & row_policy_name : query.row_policy_names->full_names)
                    res.emplace_back(AccessType::DROP_ROW_POLICY, row_policy_name.database, row_policy_name.table_name);
            }
            return res;
        }
        case AccessEntityType::QUOTA:
        {
            res.emplace_back(AccessType::DROP_QUOTA);
            return res;
        }
        case AccessEntityType::MASKING_POLICY:
        {
            res.emplace_back(AccessType::DROP_MASKING_POLICY);
            return res;
        }
        case AccessEntityType::MAX:
            break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: type is not supported by DROP query", toString(query.type));
}

void registerInterpreterDropAccessEntityQuery(InterpreterFactory & factory);
void registerInterpreterDropAccessEntityQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropAccessEntityQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropAccessEntityQuery", create_fn);
}

}
