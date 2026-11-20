#pragma once

#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <vector>

namespace DB
{

class ASTRolesOrUsersSet;
struct IAccessEntity;
struct RolesOrUsersSet;

/// Enforces the protected-entity policy for statements that target a set of
/// roles and/or users (CREATE/ALTER ROW POLICY ... TO, CREATE/ALTER QUOTA ... TO,
/// CREATE/ALTER SETTINGS PROFILE ... TO, SET DEFAULT ROLE ... TO).
///
/// Requires PROTECTED_ACCESS_MANAGEMENT if the set applies to a user that is protected or kept in a
/// read-only storage such as `users.xml`, see `AccessControl::reachesProtectedTarget`. A role in the
/// set counts through the users holding it. `TO ALL` applies to every user on the server, so an
/// unprivileged caller has to spell out `TO ALL EXCEPT <those users>` instead.
///
/// This deliberately does not refuse a set that happens to contain the caller. These
/// statements store the target set on the policy itself and never rewrite the listed users,
/// so naming yourself is not self-modification - and since `ALL` always includes the caller,
/// refusing it would break every `TO ALL` statement on the server.
void checkProtectedTargets(const ContextPtr & context, const ASTRolesOrUsersSet & targets);

/// The same check for a target set already stored on an existing row policy, quota or settings
/// profile. `ALTER` and `CREATE ... OR REPLACE` change what that entity does to its current
/// targets even when the statement has no `TO` clause.
void checkProtectedTargets(const ContextPtr & context, const RolesOrUsersSet & targets);

/// The check above on the stored `TO` set of `entity` if it is a row policy, quota or settings
/// profile, and nothing for any other entity. Dropping such an entity changes what applies to its
/// targets too: with `throw_on_unmatched_row_policies`, dropping a permissive row policy makes the
/// table unreadable for the users it named.
void checkProtectedTargetsOfEntity(const ContextPtr & context, const IAccessEntity & entity);

/// Requires PROTECTED_ACCESS_MANAGEMENT if changing one of `ids` changes what applies to a protected
/// entity, see `AccessControl::isHeldByProtectedTarget`: `ALTER ROLE` or `GRANT` on a role, or
/// `ALTER SETTINGS PROFILE` on a profile, that a protected user holds directly or through other roles
/// and profiles.
void checkNoProtectedHolders(const ContextPtr & context, const std::vector<UUID> & ids);

/// Requires PROTECTED_ACCESS_MANAGEMENT if a protected entity other than those in `ids` depends on
/// one of `ids`, directly or as in `checkNoProtectedHolders`. Removing an entity from a storage,
/// whether by `DROP`, by `CREATE ... OR REPLACE` (which gives the entity a new ID) or by `MOVE`
/// (which removes it from the source storage), makes `IAccessStorage::removeReferencesToRemovedIDs`
/// rewrite every entity that referenced it, through a path that carries no user identity. The
/// removal is refused instead.
void checkNoProtectedDependents(const ContextPtr & context, const std::vector<UUID> & ids);

/// The protected-entity check for a `CheckFunc` or `UpdateFunc`, which runs under the lock of an
/// access storage. It decides from values taken when it is constructed and never calls into
/// `ContextAccess`: the notification about a changed user takes `ContextAccess::mutex` and then
/// reads roles from the storage, so taking the two locks in the other order can deadlock.
class ProtectedEntityWriteCheck
{
public:
    explicit ProtectedEntityWriteCheck(const ContextPtr & context);

    /// Throws `ACCESS_DENIED` if the caller lacks PROTECTED_ACCESS_MANAGEMENT and `entity` is
    /// protected or the statement mentions `PROTECTED`.
    void check(const IAccessEntity & entity, bool statement_mentions_protected = false) const;

    const String & getUserName() const { return user_name; }

private:
    String user_name;
    bool has_privilege;
};

}
