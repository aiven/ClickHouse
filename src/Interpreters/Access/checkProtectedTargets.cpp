#include <Interpreters/Access/checkProtectedTargets.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

void checkProtectedTargets(const ContextPtr & context, const ASTRolesOrUsersSet & targets, bool block_self)
{
    const auto & access_control = context->getAccessControl();
    auto current_user_id = context->getUserID();

    RolesOrUsersSet resolved{targets, access_control, current_user_id};
    auto ids = resolved.getMatchingIDs(access_control);

    bool require_protected_priv = false;
    for (const auto & id : ids)
    {
        if (block_self && current_user_id && id == *current_user_id)
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "User '{}' cannot modify themselves, even with PROTECTED_ACCESS_MANAGEMENT permission",
                context->getUserName());

        if (require_protected_priv)
            continue;

        auto user = access_control.tryRead<User>(id);
        if (user && user->isProtected())
            require_protected_priv = true;
    }

    if (require_protected_priv)
        context->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
}

}
