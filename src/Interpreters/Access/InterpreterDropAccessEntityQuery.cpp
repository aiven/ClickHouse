#include <Interpreters/Access/InterpreterDropAccessEntityQuery.h>
#include <Interpreters/InterpreterFactory.h>

#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Access/Common/AccessFlags.h>
#include <Access/MaskingPolicy.h>
#include <Access/User.h>
#include <Access/ViewDefinerDependencies.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int HAVE_DEPENDENT_OBJECTS;
    extern const int ACCESS_DENIED;
}


BlockIO InterpreterDropAccessEntityQuery::execute()
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query_ptr->as<ASTDropAccessEntityQuery &>();

    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(getRequiredAccess());

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    IAccessStorage * storage = &access_control;
    MultipleAccessStorage::StoragePtr storage_ptr;
    if (!query.storage_name.empty())
    {
        storage_ptr = access_control.getStorageByName(query.storage_name);
        storage = storage_ptr.get();
    }

    auto access_ptr = getContext()->getAccess();
    String current_user_name = getContext()->getUserName();

    auto check_func = [access_ptr, current_user_name](const AccessEntityPtr & entity)
    {
        if (entity->getType() == AccessEntityType::USER && entity->getName() == current_user_name)
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "User `{}` cannot drop themselves, even with PROTECTED_ACCESS_MANAGEMENT permission",
                current_user_name);

        if (entity->isProtected())
            access_ptr->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
    };

    /// Enforce self-protection and the protected-user policy on the initiator before any
    /// ON CLUSTER dispatch, so the check cannot be bypassed via DDLWorker.
    {
        Strings names_to_check;
        if (query.type == AccessEntityType::ROW_POLICY)
            names_to_check = query.row_policy_names->toStrings();
        else if (query.type == AccessEntityType::MASKING_POLICY)
            names_to_check = Strings{query.masking_policy_name->toString()};
        else
            names_to_check = query.names;

        for (const auto & id : storage->find(query.type, names_to_check))
        {
            if (auto entity = storage->tryRead(id))
                check_func(entity);
        }
    }

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());

    /// `check_func` is still passed to remove() on the local path as defense in depth.
    auto do_drop = [&](const Strings & names)
    {
        if (query.if_exists)
        {
            for (const auto & id : storage->find(query.type, names))
                storage->remove(id, check_func);
        }
        else
        {
            for (const auto & id : storage->getIDs(query.type, names))
                storage->remove(id, check_func);
        }
    };

    if (query.type == AccessEntityType::USER)
    {
        auto & view_definer_dependencies = ViewDefinerDependencies::instance();
        for (const auto & name : query.names)
        {
            if (view_definer_dependencies.hasViewDependencies(name))
            {
                auto views_storage_ids = view_definer_dependencies.getViewsForDefiner(name);
                std::vector<String> views;
                views.reserve(views_storage_ids.size());
                for (const auto & id : views_storage_ids)
                    views.push_back(id.getNameForLogs());
                throw Exception(ErrorCodes::HAVE_DEPENDENT_OBJECTS, "User `{}` is used as a definer in views {}.", name, toString(views));
            }
        }
    }

    if (query.type == AccessEntityType::ROW_POLICY)
        do_drop(query.row_policy_names->toStrings());
    else if (query.type == AccessEntityType::MASKING_POLICY)
        do_drop(Strings{query.masking_policy_name->toString()});
    else
        do_drop(query.names);

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

void registerInterpreterDropAccessEntityQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropAccessEntityQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropAccessEntityQuery", create_fn);
}

}
