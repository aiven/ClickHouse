#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

class ASTRolesOrUsersSet;

/// Enforces the protected-entity policy for statements that target a set of
/// roles and/or users (CREATE/ALTER ROW POLICY ... TO, CREATE/ALTER QUOTA ... TO,
/// CREATE/ALTER SETTINGS PROFILE ... TO, SET DEFAULT ROLE ... TO).
///
/// Resolves `targets` to access-entity IDs and:
///  - throws ACCESS_DENIED if `block_self` and the current user is among the targets;
///  - requires PROTECTED_ACCESS_MANAGEMENT if any target is a protected user or role.
void checkProtectedTargets(const ContextPtr & context, const ASTRolesOrUsersSet & targets, bool block_self);

}
