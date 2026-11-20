#include <Interpreters/Access/checkProtectedTargets.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Access/IAccessEntity.h>
#include <Access/Quota.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/RowPolicy.h>
#include <Access/SettingsProfile.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

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

    /// Always throws: the early return above established that the privilege is missing.
    if (context->getAccessControl().reachesProtectedTarget(targets))
        context->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
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

    /// Exceptions from the scan propagate, so an unreadable entity refuses the removal rather than
    /// silently allowing it. Always throws: the early return above established that the privilege is missing.
    if (context->getAccessControl().hasProtectedDependents(ids))
        context->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
}

void checkNoProtectedHolders(const ContextPtr & context, const std::vector<UUID> & ids)
{
    if (ids.empty() || context->getAccess()->isGranted(AccessType::PROTECTED_ACCESS_MANAGEMENT))
        return;

    /// Always throws: the early return above established that the privilege is missing.
    if (context->getAccessControl().isHeldByProtectedTarget(ids))
        context->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
}

ProtectedEntityWriteCheck::ProtectedEntityWriteCheck(const ContextPtr & context)
    : user_name(context->getUserName())
    , has_privilege(context->getAccess()->isGranted(AccessType::PROTECTED_ACCESS_MANAGEMENT))
{
}

void ProtectedEntityWriteCheck::check(const IAccessEntity & entity, bool statement_mentions_protected) const
{
    if (has_privilege || (!entity.isProtected() && !statement_mentions_protected))
        return;

    /// The wording of `ContextAccess`, which this must not call into.
    throw Exception(ErrorCodes::ACCESS_DENIED,
        "{}: Not enough privileges. To execute this query, it's necessary to have the grant {}",
        user_name, AccessRightsElement{AccessType::PROTECTED_ACCESS_MANAGEMENT}.toStringWithoutOptions());
}

}
