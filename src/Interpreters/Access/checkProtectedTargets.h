#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class ASTRolesOrUsersSet;

/// Enforces the protected-user policy for statements that target a set of users
/// (CREATE/ALTER ROW POLICY ... TO, CREATE/ALTER QUOTA ... TO,
/// CREATE/ALTER SETTINGS PROFILE ... TO, SET DEFAULT ROLE ... TO).
///
/// Resolves `targets` to user IDs and:
///  - throws ACCESS_DENIED if `block_self` and the current user is among the targets;
///  - requires PROTECTED_ACCESS_MANAGEMENT if any target is a protected user.
void checkProtectedTargets(const ContextPtr & context, const ASTRolesOrUsersSet & targets, bool block_self);

}
