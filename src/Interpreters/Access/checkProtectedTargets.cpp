#include <Interpreters/Access/checkProtectedTargets.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Access/IAccessEntity.h>
#include <Access/Quota.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/RowPolicy.h>
#include <Access/SettingsProfile.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <base/range.h>

#include <unordered_set>

namespace DB
{

void checkProtectedTargets(const ContextPtr & context, const ASTRolesOrUsersSet & targets)
{
    if (context->getAccess()->isGranted(AccessType::PROTECTED_ACCESS_MANAGEMENT))
        return;

    checkProtectedTargets(context, RolesOrUsersSet{targets, context->getAccessControl(), context->getUserID()});
}

void checkProtectedTargets(const ContextPtr & context, const RolesOrUsersSet & targets)
{
    /// Holding the privilege makes the scan below pointless, and the scan is the expensive
    /// part: `TO ALL` resolves to every user and role on the server.
    if (context->getAccess()->isGranted(AccessType::PROTECTED_ACCESS_MANAGEMENT))
        return;

    const auto & access_control = context->getAccessControl();

    for (const auto & id : targets.getMatchingIDs(access_control))
    {
        /// Always throws: the early return above established that the privilege is missing.
        if (access_control.isProtectedTarget(id))
            context->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
    }
}

void checkProtectedTargetsOfEntity(const ContextPtr & context, const IAccessEntity & entity)
{
    if (const auto * policy = typeid_cast<const RowPolicy *>(&entity))
        checkProtectedTargets(context, policy->to_roles);
    else if (const auto * quota = typeid_cast<const Quota *>(&entity))
        checkProtectedTargets(context, quota->to_roles);
    else if (const auto * profile = typeid_cast<const SettingsProfile *>(&entity))
        checkProtectedTargets(context, profile->to_roles);
}

void checkNoProtectedDependents(const ContextPtr & context, const std::vector<UUID> & ids)
{
    if (ids.empty() || context->getAccess()->isGranted(AccessType::PROTECTED_ACCESS_MANAGEMENT))
        return;

    const auto & access_control = context->getAccessControl();
    const std::unordered_set<UUID> removed_ids(ids.begin(), ids.end());

    /// Exceptions from the scan propagate, so an unreadable entity refuses the removal rather than
    /// silently allowing it.
    for (auto dependent_type : collections::range(AccessEntityType::MAX))
    {
        for (const auto & dependent_id : access_control.findAll(dependent_type))
        {
            if (removed_ids.contains(dependent_id))
                continue;

            auto dependent = access_control.tryRead(dependent_id);
            if (!dependent || !dependent->isProtected() || !dependent->hasDependencies(removed_ids))
                continue;

            /// Always throws: the early return above established that the privilege is missing.
            context->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
        }
    }
}

}
