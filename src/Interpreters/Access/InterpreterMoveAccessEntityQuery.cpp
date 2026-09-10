#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ACCESS_ENTITY_NOT_FOUND;
    extern const int ACCESS_DENIED;
}


BlockIO InterpreterMoveAccessEntityQuery::execute()
{
    auto & query = query_ptr->as<ASTMoveAccessEntityQuery &>();
    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(getRequiredAccess());

    /// Aiven patch 022. Moving an entity between storages rewrites it (drop here, create there),
    /// so it must obey the protected-entity policy - and the check has to run on the initiator,
    /// before the ON CLUSTER dispatch below, or `MOVE ... ON CLUSTER` would launder it through
    /// `DDLWorker`. `find` rather than `getIDs`: an entity absent on the initiator is not an
    /// error at this point, only a target we have nothing to check.
    {
        Strings names_to_check;
        if (query.type == AccessEntityType::ROW_POLICY)
        {
            /// Qualify the database for the check only; the distributed AST must stay unqualified.
            auto names_for_check = boost::static_pointer_cast<ASTRowPolicyNames>(query.row_policy_names->clone());
            names_for_check->replaceEmptyDatabase(getContext()->getCurrentDatabase());
            names_to_check = names_for_check->toStrings();
        }
        else
            names_to_check = query.names;

        const auto current_user_id = getContext()->getUserID();
        bool require_protected_priv = false;
        for (const auto & id : access_control.find(query.type, names_to_check))
        {
            if (current_user_id && id == *current_user_id)
                throw Exception(ErrorCodes::ACCESS_DENIED,
                    "User '{}' cannot move themselves, even with PROTECTED_ACCESS_MANAGEMENT permission",
                    getContext()->getUserName());

            auto entity = access_control.tryRead(id);
            if (entity && entity->isProtected())
                require_protected_priv = true;
        }
        if (require_protected_priv)
            getContext()->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
    }

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext());

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    std::vector<UUID> ids;
    if (query.type == AccessEntityType::ROW_POLICY)
        ids = access_control.getIDs(query.type, query.row_policy_names->toStrings());
    else
        ids = access_control.getIDs(query.type, query.names);

    /// Validate that all entities are from the same storage.
    const auto source_storage = access_control.findStorage(ids.front());
    if (!source_storage->exists(ids))
        throw Exception(ErrorCodes::ACCESS_ENTITY_NOT_FOUND, "All access entities must be from the same storage in order to be moved");

    access_control.moveAccessEntities(ids, source_storage->getStorageName(), query.storage_name);
    return {};
}


AccessRightsElements InterpreterMoveAccessEntityQuery::getRequiredAccess() const
{
    const auto & query = query_ptr->as<const ASTMoveAccessEntityQuery &>();
    AccessRightsElements res;
    switch (query.type)
    {
        case AccessEntityType::USER:
        {
            for (const auto & name: query.names)
            {
                res.emplace_back(AccessType::DROP_USER, name);
                res.emplace_back(AccessType::CREATE_USER, name);
            }
            return res;
        }
        case AccessEntityType::ROLE:
        {
            for (const auto & name: query.names)
            {
                res.emplace_back(AccessType::DROP_ROLE, name);
                res.emplace_back(AccessType::CREATE_ROLE, name);
            }
            return res;
        }
        case AccessEntityType::SETTINGS_PROFILE:
        {
            res.emplace_back(AccessType::DROP_SETTINGS_PROFILE);
            res.emplace_back(AccessType::CREATE_SETTINGS_PROFILE);
            return res;
        }
        case AccessEntityType::ROW_POLICY:
        {
            if (query.row_policy_names)
            {
                for (const auto & row_policy_name : query.row_policy_names->full_names)
                {
                    res.emplace_back(AccessType::DROP_ROW_POLICY, row_policy_name.database, row_policy_name.table_name);
                    res.emplace_back(AccessType::CREATE_ROW_POLICY, row_policy_name.database, row_policy_name.table_name);
                }
            }
            return res;
        }
        case AccessEntityType::QUOTA:
        {
            res.emplace_back(AccessType::DROP_QUOTA);
            res.emplace_back(AccessType::CREATE_QUOTA);
            return res;
        }
        case AccessEntityType::MASKING_POLICY:
        {
            res.emplace_back(AccessType::DROP_MASKING_POLICY);
            res.emplace_back(AccessType::CREATE_MASKING_POLICY);
            return res;
        }
        case AccessEntityType::MAX:
            break;
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: type is not supported by DROP query", toString(query.type));
}

void registerInterpreterMoveAccessEntityQuery(InterpreterFactory & factory);
void registerInterpreterMoveAccessEntityQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterMoveAccessEntityQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterMoveAccessEntityQuery", create_fn);
}

}
