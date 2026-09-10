#include <Interpreters/Access/checkProtectedTargets.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Access/IAccessEntity.h>
#include <Access/RolesOrUsersSet.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>

namespace DB
{

void checkProtectedTargets(const ContextPtr & context, const ASTRolesOrUsersSet & targets)
{
    /// Holding the privilege makes the scan below pointless, and the scan is the expensive
    /// part: `TO ALL` resolves to every user and role on the server.
    if (context->getAccess()->isGranted(AccessType::PROTECTED_ACCESS_MANAGEMENT))
        return;

    const auto & access_control = context->getAccessControl();
    RolesOrUsersSet resolved{targets, access_control, context->getUserID()};

    for (const auto & id : resolved.getMatchingIDs(access_control))
    {
        /// Read type-erased, through IAccessEntity and the virtual isProtected. The `TO`
        /// clause of a row policy, quota or settings profile is parsed with
        /// allowRoles().allowUsers(), so a role is a legitimate target here and reading it
        /// as a User would raise LOGICAL_ERROR.
        auto entity = access_control.tryRead<IAccessEntity>(id);

        /// Always throws: the early return above established that the privilege is missing.
        if (entity && entity->isProtected())
            context->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
    }
}

}
