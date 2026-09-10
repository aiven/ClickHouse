#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class ASTRolesOrUsersSet;

/// Enforces the protected-entity policy for statements that target a set of
/// roles and/or users (CREATE/ALTER ROW POLICY ... TO, CREATE/ALTER QUOTA ... TO,
/// CREATE/ALTER SETTINGS PROFILE ... TO, SET DEFAULT ROLE ... TO).
///
/// Resolves `targets` to access-entity IDs and requires PROTECTED_ACCESS_MANAGEMENT if any
/// of them is protected. `TO ALL` resolves to every user and role on the server, so it does
/// reach protected entities and an unprivileged caller has to spell out
/// `TO ALL EXCEPT <protected entities>` instead.
///
/// This deliberately does not refuse a set that happens to contain the caller. These
/// statements store the target set on the policy itself and never rewrite the listed users,
/// so naming yourself is not self-modification - and since `ALL` always includes the caller,
/// refusing it would break every `TO ALL` statement on the server.
void checkProtectedTargets(const ContextPtr & context, const ASTRolesOrUsersSet & targets);

}
