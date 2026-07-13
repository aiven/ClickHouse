#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterGrantQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/Common/AccessType.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Role.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <Storages/StorageFactory.h>
#include <Core/ServerSettings.h>
#include <Common/quoteString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ACCESS_DENIED;
}

namespace ServerSetting
{
    extern const ServerSettingsString cluster_database;
}

namespace
{
    /// Extracts access rights elements which are going to be granted or revoked from a query.
    /// elements_to_revoke: applied BEFORE the grant (for REPLACE: revoke ALL, or standalone REVOKE).
    /// elements_to_revoke_after_grant: applied AFTER the grant (combined syntax revokes -> creates partial revokes).
    void collectAccessRightsElementsToGrantOrRevoke(
        const ASTGrantQuery & query,
        AccessRightsElements & elements_to_grant,
        AccessRightsElements & elements_to_revoke,
        AccessRightsElements & elements_to_revoke_after_grant)
    {
        elements_to_grant.clear();
        elements_to_revoke.clear();
        elements_to_revoke_after_grant.clear();

        if (query.is_revoke)
        {
            /// REVOKE
            elements_to_revoke = query.access_rights_elements;
        }
        else if (query.replace_access)
        {
            /// GRANT WITH REPLACE OPTION
            elements_to_grant = query.access_rights_elements;
            elements_to_revoke.emplace_back(AccessType::ALL);
            /// Explicit revokes from combined syntax are applied after the grant
            elements_to_revoke_after_grant = query.access_rights_elements_to_revoke;
        }
        else
        {
            /// GRANT (possibly with embedded EXCEPT)
            elements_to_grant = query.access_rights_elements;
            /// Combined syntax revokes are applied after the grant to create partial revokes
            elements_to_revoke_after_grant = query.access_rights_elements_to_revoke;
        }
    }

    /// Extracts roles which are going to be granted or revoked from a query.
    void collectRolesToGrantOrRevoke(
        const AccessControl & access_control,
        const ASTGrantQuery & query,
        std::vector<UUID> & roles_to_grant,
        RolesOrUsersSet & roles_to_revoke)
    {
        roles_to_grant.clear();
        roles_to_revoke.clear();

        RolesOrUsersSet roles_to_grant_or_revoke;
        if (query.roles)
            roles_to_grant_or_revoke = RolesOrUsersSet{*query.roles, access_control};

        if (query.is_revoke)
        {
            /// REVOKE
            roles_to_revoke = std::move(roles_to_grant_or_revoke);
        }
        else if (query.replace_granted_roles)
        {
            /// GRANT WITH REPLACE OPTION
            roles_to_grant = roles_to_grant_or_revoke.getMatchingIDs(access_control);
            roles_to_revoke = RolesOrUsersSet::AllTag{};
        }
        else
        {
            /// GRANT
            roles_to_grant = roles_to_grant_or_revoke.getMatchingIDs(access_control);
        }
    }

    /// Extracts roles which are going to be granted or revoked from a query.
    void collectRolesToGrantOrRevoke(
        const ASTGrantQuery & query,
        std::vector<UUID> & roles_to_grant,
        RolesOrUsersSet & roles_to_revoke)
    {
        roles_to_grant.clear();
        roles_to_revoke.clear();

        RolesOrUsersSet roles_to_grant_or_revoke;
        if (query.roles)
            roles_to_grant_or_revoke = RolesOrUsersSet{*query.roles};

        if (query.is_revoke)
        {
            /// REVOKE
            roles_to_revoke = std::move(roles_to_grant_or_revoke);
        }
        else if (query.replace_granted_roles)
        {
            /// GRANT WITH REPLACE OPTION
            roles_to_grant = roles_to_grant_or_revoke.getMatchingIDs();
            roles_to_revoke = RolesOrUsersSet::AllTag{};
        }
        else
        {
            /// GRANT
            roles_to_grant = roles_to_grant_or_revoke.getMatchingIDs();
        }
    }

    /// Checks if the current user has enough access rights granted with grant option to grant or revoke specified access rights.
    void checkGrantOption(
        const AccessControl & access_control,
        const ContextAccessWrapper & current_user_access,
        const std::vector<UUID> & grantees_from_query,
        bool & need_check_grantees_are_allowed,
        const AccessRightsElements & elements_to_grant,
        AccessRightsElements & elements_to_revoke,
        AccessRightsElements & elements_to_revoke_after_grant)
    {
        /// Check access rights which are going to be granted.
        /// To execute the command GRANT the current user needs to have the access granted with GRANT OPTION.
        current_user_access.checkGrantOption(elements_to_grant);

        /// Combine both revoke lists purely for the permission check below.
        ///
        /// NOTE: this combined list is intentionally local and is never written back to `elements_to_revoke`
        /// or `elements_to_revoke_after_grant`. The narrowing performed further down (intersecting the requested
        /// revokes with the access the grantees *currently* have) is only used to decide whether the current user
        /// is allowed to run the command; it must not be propagated to the elements that are actually applied:
        ///   - `elements_to_revoke_after_grant` (the combined GRANT ... EXCEPT ... syntax) targets rights that are
        ///     granted by this very statement, so the grantees do not hold them yet. Narrowing against their current
        ///     access would drop those elements and silently ignore the EXCEPT clause.
        ///   - `elements_to_revoke` (REVOKE / REPLACE) yields an identical final state whether the narrowed or the
        ///     full list is applied, because revoking rights a grantee does not hold is a no-op.
        AccessRightsElements all_elements_to_revoke;
        all_elements_to_revoke.insert(all_elements_to_revoke.end(), elements_to_revoke.begin(), elements_to_revoke.end());
        all_elements_to_revoke.insert(all_elements_to_revoke.end(), elements_to_revoke_after_grant.begin(), elements_to_revoke_after_grant.end());

        if (current_user_access.hasGrantOption(all_elements_to_revoke))
        {
            /// Simple case: the current user has the grant option for all the access rights specified for REVOKE.
            return;
        }

        /// Special case for the command REVOKE: it's possible that the current user doesn't have
        /// the access granted with GRANT OPTION but it's still ok because the roles or users
        /// from whom the access rights will be revoked don't have the specified access granted either.
        ///
        /// For example, to execute
        /// GRANT ALL ON mydb.* TO role1
        /// REVOKE ALL ON *.* FROM role1
        /// the current user needs to have the grants only on the 'mydb' database.
        AccessRights all_granted_access;
        for (const auto & id : grantees_from_query)
        {
            auto entity = access_control.tryRead(id);
            if (auto role = typeid_cast<RolePtr>(entity))
            {
                if (need_check_grantees_are_allowed)
                    current_user_access.checkGranteeIsAllowed(id, *role);
                all_granted_access.makeUnion(role->access);
            }
            else if (auto user = typeid_cast<UserPtr>(entity))
            {
                if (need_check_grantees_are_allowed)
                    current_user_access.checkGranteeIsAllowed(id, *user);
                all_granted_access.makeUnion(user->access);
            }
        }

        need_check_grantees_are_allowed = false; /// already checked

        if (!all_elements_to_revoke.empty() && all_elements_to_revoke[0].is_partial_revoke)
            std::for_each(all_elements_to_revoke.begin(), all_elements_to_revoke.end(), [&](AccessRightsElement & element) { element.is_partial_revoke = false; });
        AccessRights access_to_revoke;
        access_to_revoke.grant(all_elements_to_revoke);
        access_to_revoke.makeIntersection(all_granted_access);

        /// Build more accurate list of elements to revoke, now we use an intersection of the initial list of elements to revoke
        /// and all the granted access rights to these grantees.
        bool grant_option = !all_elements_to_revoke.empty() && all_elements_to_revoke[0].grant_option;
        all_elements_to_revoke.clear();
        for (auto & element_to_revoke : access_to_revoke.getElements())
        {
            if (!element_to_revoke.is_partial_revoke && (element_to_revoke.grant_option || !grant_option))
                all_elements_to_revoke.emplace_back(std::move(element_to_revoke));
        }

        /// Additional check for REVOKE
        ///
        /// If user1 has the rights
        /// GRANT SELECT ON *.* TO user1;
        /// REVOKE SELECT ON system.* FROM user1;
        /// REVOKE SELECT ON mydb.* FROM user1;
        ///
        /// And user2 has the rights
        /// GRANT SELECT ON *.* TO user2;
        /// REVOKE SELECT ON system.* FROM user2;
        ///
        /// the query `REVOKE SELECT ON *.* FROM user1` executed by user2 should succeed.
        if (current_user_access.getAccessRightsWithImplicit()->containsWithGrantOption(access_to_revoke))
            return;

        /// Technically, this check always fails if `containsWithGrantOption` returns `false`. But we still call it to get a nice exception message.
        current_user_access.checkGrantOption(all_elements_to_revoke);
    }

    /// Checks if the current user has enough roles granted with admin option to grant or revoke specified roles.
    void checkAdminOption(
        const AccessControl & access_control,
        const ContextAccessWrapper & current_user_access,
        const std::vector<UUID> & grantees_from_query,
        bool & need_check_grantees_are_allowed,
        const std::vector<UUID> & roles_to_grant,
        RolesOrUsersSet & roles_to_revoke,
        bool admin_option)
    {
        /// Check roles which are going to be granted.
        /// To execute the command GRANT the current user needs to have the roles granted with ADMIN OPTION.
        current_user_access.checkAdminOption(roles_to_grant);

        /// Check roles which are going to be revoked.
        std::vector<UUID> roles_to_revoke_ids;
        if (!roles_to_revoke.all)
        {
            roles_to_revoke_ids = roles_to_revoke.getMatchingIDs();
            if (current_user_access.hasAdminOption(roles_to_revoke_ids))
            {
                /// Simple case: the current user has the admin option for all the roles specified for REVOKE.
                return;
            }
        }

        /// Special case for the command REVOKE: it's possible that the current user doesn't have the admin option
        /// for some of the specified roles but it's still ok because the roles or users from whom the roles will be
        /// revoked from don't have the specified roles granted either.
        ///
        /// For example, to execute
        /// GRANT role2 TO role1
        /// REVOKE ALL FROM role1
        /// the current user needs to have only 'role2' to be granted with admin option (not all the roles).
        GrantedRoles all_granted_roles;
        for (const auto & id : grantees_from_query)
        {
            auto entity = access_control.tryRead(id);
            if (auto role = typeid_cast<RolePtr>(entity))
            {
                if (need_check_grantees_are_allowed)
                    current_user_access.checkGranteeIsAllowed(id, *role);
                all_granted_roles.makeUnion(role->granted_roles);
            }
            else if (auto user = typeid_cast<UserPtr>(entity))
            {
                if (need_check_grantees_are_allowed)
                    current_user_access.checkGranteeIsAllowed(id, *user);
                all_granted_roles.makeUnion(user->granted_roles);
            }
        }

        need_check_grantees_are_allowed = false; /// already checked

        const auto & all_granted_roles_set = admin_option ? all_granted_roles.getGrantedWithAdminOption() : all_granted_roles.getGranted();
        if (roles_to_revoke.all)
            boost::range::set_difference(all_granted_roles_set, roles_to_revoke.except_ids, std::back_inserter(roles_to_revoke_ids));
        else
            std::erase_if(roles_to_revoke_ids, [&](const UUID & id) { return !all_granted_roles_set.count(id); });

        roles_to_revoke = roles_to_revoke_ids;
        current_user_access.checkAdminOption(roles_to_revoke_ids);
    }

    /// Returns access rights which should be checked for executing GRANT/REVOKE on cluster.
    /// This function is less accurate than checkGrantOption() because it cannot use any information about
    /// access rights the grantees currently have (due to those grantees are located on multiple nodes,
    /// we just don't have the full information about them).
    AccessRightsElements getRequiredAccessForExecutingOnCluster(const AccessRightsElements & elements_to_grant, const AccessRightsElements & elements_to_revoke, const AccessRightsElements & elements_to_revoke_after_grant)
    {
        auto required_access = elements_to_grant;
        required_access.insert(required_access.end(), elements_to_revoke.begin(), elements_to_revoke.end());
        required_access.insert(required_access.end(), elements_to_revoke_after_grant.begin(), elements_to_revoke_after_grant.end());
        std::for_each(required_access.begin(), required_access.end(), [&](AccessRightsElement & element) { element.grant_option = true; });
        return required_access;
    }

    /// Checks if the current user has enough roles granted with admin option to grant or revoke specified roles on cluster.
    /// This function is less accurate than checkAdminOption() because it cannot use any information about
    /// granted roles the grantees currently have (due to those grantees are located on multiple nodes,
    /// we just don't have the full information about them).
    void checkAdminOptionForExecutingOnCluster(const ContextAccessWrapper & current_user_access,
                                               const std::vector<UUID> roles_to_grant,
                                               const RolesOrUsersSet & roles_to_revoke)
    {
        if (roles_to_revoke.all)
        {
            /// Revoking all the roles on cluster always requires ROLE_ADMIN privilege
            /// because when we send the query REVOKE ALL to each shard we don't know at this point
            /// which roles exactly this is going to revoke on each shard.
            /// However ROLE_ADMIN just allows to revoke every role, that's why we check it here.
            current_user_access.checkAccess(AccessType::ROLE_ADMIN);
            return;
        }

        if (current_user_access.isGranted(AccessType::ROLE_ADMIN))
            return;

        for (const auto & role_id : roles_to_grant)
            current_user_access.checkAdminOption(role_id);


        for (const auto & role_id : roles_to_revoke.getMatchingIDs())
            current_user_access.checkAdminOption(role_id);
    }

    template <typename T>
    void updateGrantedAccessRightsAndRolesTemplate(
        T & grantee,
        const AccessRightsElements & elements_to_grant,
        const AccessRightsElements & elements_to_revoke,
        const AccessRightsElements & elements_to_revoke_after_grant,
        const std::vector<UUID> & roles_to_grant,
        const RolesOrUsersSet & roles_to_revoke,
        bool admin_option)
    {
        /// Step 1: Pre-grant revoke (for REPLACE: revoke ALL, or standalone REVOKE).
        if (!elements_to_revoke.empty())
            grantee.access.revoke(elements_to_revoke);

        /// Step 2: Grant.
        if (!elements_to_grant.empty())
            grantee.access.grant(elements_to_grant);

        /// Step 3: Post-grant revoke (for combined GRANT ... EXCEPT ... syntax -> creates partial revokes).
        if (!elements_to_revoke_after_grant.empty())
            grantee.access.revoke(elements_to_revoke_after_grant);

        if (!roles_to_revoke.empty())
        {
            if (admin_option)
            {
                grantee.granted_roles.revokeAdminOption(grantee.granted_roles.findGrantedWithAdminOption(roles_to_revoke));
            }
            else
            {
                auto found_roles_to_revoke = grantee.granted_roles.findGranted(roles_to_revoke);
                grantee.granted_roles.revoke(found_roles_to_revoke);

                if constexpr (std::is_same_v<T, User>)
                {
                    for (const auto & id : found_roles_to_revoke)
                        grantee.default_roles.ids.erase(id);
                }
            }
        }

        if (!roles_to_grant.empty())
        {
            if (admin_option)
                grantee.granted_roles.grantWithAdminOption(roles_to_grant);
            else
                grantee.granted_roles.grant(roles_to_grant);
        }
    }

    /// Updates grants of a specified user or role.
    void updateGrantedAccessRightsAndRoles(
        IAccessEntity & grantee,
        const AccessRightsElements & elements_to_grant,
        const AccessRightsElements & elements_to_revoke,
        const AccessRightsElements & elements_to_revoke_after_grant,
        const std::vector<UUID> & roles_to_grant,
        const RolesOrUsersSet & roles_to_revoke,
        bool admin_option)
    {
        if (auto * user = typeid_cast<User *>(&grantee))
            updateGrantedAccessRightsAndRolesTemplate(*user, elements_to_grant, elements_to_revoke, elements_to_revoke_after_grant, roles_to_grant, roles_to_revoke, admin_option);
        else if (auto * role = typeid_cast<Role *>(&grantee))
            updateGrantedAccessRightsAndRolesTemplate(*role, elements_to_grant, elements_to_revoke, elements_to_revoke_after_grant, roles_to_grant, roles_to_revoke, admin_option);
    }

    template <typename T>
    void grantCurrentGrantsTemplate(
        T & grantee,
        const AccessRights & rights_to_grant,
        const AccessRightsElements & elements_to_revoke,
        const AccessRightsElements & elements_to_revoke_after_grant)
    {
        if (!elements_to_revoke.empty())
            grantee.access.revoke(elements_to_revoke);

        grantee.access.makeUnion(rights_to_grant);

        if (!elements_to_revoke_after_grant.empty())
            grantee.access.revoke(elements_to_revoke_after_grant);
    }

    /// Grants current user's grants with grant options to specified user.
    void grantCurrentGrants(
        IAccessEntity & grantee,
        const AccessRights & new_rights,
        const AccessRightsElements & elements_to_revoke,
        const AccessRightsElements & elements_to_revoke_after_grant)
    {
        if (auto * user = typeid_cast<User *>(&grantee))
            grantCurrentGrantsTemplate(*user, new_rights, elements_to_revoke, elements_to_revoke_after_grant);
        else if (auto * role = typeid_cast<Role *>(&grantee))
            grantCurrentGrantsTemplate(*role, new_rights, elements_to_revoke, elements_to_revoke_after_grant);
    }

    /// Calculates all available rights to grant with current user intersection.
    void calculateCurrentGrantRightsWithIntersection(
        AccessRights & rights,
        std::shared_ptr<const ContextAccessWrapper> current_user_access,
        const AccessRightsElements & elements_to_grant)
    {
        auto current_user_grantable_rights = current_user_access->getAccessRights()->getGrantableRights();
        rights.grant(elements_to_grant);
        rights.makeIntersection(current_user_grantable_rights);
    }

    /// Updates grants of a specified user or role.
    void updateFromQuery(IAccessEntity & grantee, const ASTGrantQuery & query)
    {
        AccessRightsElements elements_to_grant;
        AccessRightsElements elements_to_revoke;
        AccessRightsElements elements_to_revoke_after_grant;
        collectAccessRightsElementsToGrantOrRevoke(query, elements_to_grant, elements_to_revoke, elements_to_revoke_after_grant);

        std::vector<UUID> roles_to_grant;
        RolesOrUsersSet roles_to_revoke;
        collectRolesToGrantOrRevoke(query, roles_to_grant, roles_to_revoke);

        updateGrantedAccessRightsAndRoles(grantee, elements_to_grant, elements_to_revoke, elements_to_revoke_after_grant, roles_to_grant, roles_to_revoke, query.admin_option);
    }
}


BlockIO InterpreterGrantQuery::execute()
{
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTGrantQuery &>();

    /// `GRANT DEFAULT REPLICATED DATABASE PRIVILEGES` is a shortcut that expands to a fixed
    /// privilege set granted on a database. It must be handled before `eraseNotGrantable` and the
    /// TABLE ENGINE validation below: those operate on the synthetic `AccessType::ALL` element the
    /// parser produced for this statement and would strip or reject it. We rebuild a concrete GRANT
    /// string and execute it internally instead.
    if (query.default_replicated_db_privileges)
    {
        auto context = getContext();
        if (query.access_rights_elements.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected number of access rights elements: {}.", query.access_rights_elements.size());
        String db_name = query.access_rights_elements[0].database;
        if (query.grantees->names.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected number of grantees.");
        String grantee = query.grantees->names[0];
        /// We cannot check if database is replicated here because it might not be created yet.

        String cluster_database = context->getServerSettings()[ServerSetting::cluster_database];
        String default_grant_query = "GRANT ";
        if (db_name != cluster_database)
            default_grant_query += "DROP DATABASE, ";
        default_grant_query +=
            "ALTER UPDATE, "
            "ALTER DELETE, "
            "ALTER COLUMN, "
            "ALTER MODIFY COMMENT, "
            "ALTER INDEX, "
            "ALTER PROJECTION, "
            "ALTER CONSTRAINT, "
            "ALTER TTL, "
            "ALTER MATERIALIZE TTL, "
            "ALTER SETTINGS, "
            "ALTER MOVE PARTITION, "
            "ALTER FETCH PARTITION, "
            "ALTER VIEW, "
            // CREATE TABLE implicitly enables CREATE VIEW
            "CREATE TABLE, "
            // DROP TABLE implicitly enables DROP VIEW
            "DROP TABLE, "
            "CREATE DICTIONARY, "
            "DROP DICTIONARY, "
            "dictGet, "
            "INSERT, "
            "OPTIMIZE, "
            "SELECT, "
            "SHOW, "
            "CHECK, "
            "SYSTEM SYNC REPLICA, "
            "TRUNCATE "
            "ON " + backQuote(db_name) + ".* TO " + backQuote(grantee) + " WITH GRANT OPTION";

        /// Run the expanded GRANT as an internal query with a fresh `query_id`. The outer GRANT is still
        /// registered in the process list under its own id; on 26.3 internal queries are registered too, so
        /// reusing the outer id here self-collides. A throwaway copy isolates the registration identity while
        /// inheriting the (possibly elevated) access and settings unchanged.
        auto grant_context = Context::createCopy(context);
        grant_context->setCurrentQueryId("");
        executeQuery(default_grant_query, grant_context, QueryFlags{ .internal = true });
        return {};
    }

    query.replaceCurrentUserTag(getContext()->getUserName());
    query.access_rights_elements.eraseNotGrantable();
    query.access_rights_elements_to_revoke.eraseNotGrantable();

    if (!query.access_rights_elements.sameOptions())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Elements of an ASTGrantQuery are expected to have the same options");
    if (!query.access_rights_elements.empty() && query.access_rights_elements[0].is_partial_revoke && !query.is_revoke)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A partial revoke should be revoked, not granted");
    if (!query.access_rights_elements_to_revoke.sameOptions())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Revoke elements of an ASTGrantQuery are expected to have the same options");

    auto & access_control = getContext()->getAccessControl();
    auto current_user_access = getContext()->getAccess();
    String current_user_name = getContext()->getUserName();
    std::optional<UUID> current_user_id_opt = getContext()->getUserID();

    /// Validate TABLE ENGINE parameter names if explicitly specified
    for (const auto & element : query.access_rights_elements)
    {
        if (element.isGlobalWithParameter()
            && (element.access_flags.getParameterType() == AccessFlags::TABLE_ENGINE)
            && !element.anyParameter())
        {
            /// Will throw UNKNOWN_STORAGE if engine is unknown
            (void)StorageFactory::instance().getStorageFeatures(element.parameter);
        }
    }

    std::vector<UUID> grantees = RolesOrUsersSet{*query.grantees, access_control, current_user_id_opt}.getMatchingIDs(access_control);

    /// Enforce self-protection and the protected-user policy on the initiator before any
    /// ON CLUSTER dispatch, so the check cannot be bypassed via DDLWorker. We compare each
    /// grantee against the current user by BOTH UUID and name, because the resolved grantee
    /// UUID can differ from `getContext()->getUserID` (e.g. when addressed by name).
    {
        bool requires_protected_priv = false;
        for (const auto & grantee_id : grantees)
        {
            const bool is_self_by_uuid = current_user_id_opt && grantee_id == *current_user_id_opt;
            auto grantee_entity = access_control.tryRead(grantee_id);
            const bool is_self_by_name = grantee_entity
                && grantee_entity->getType() == AccessEntityType::USER
                && grantee_entity->getName() == current_user_name;

            if (query.is_revoke && (is_self_by_uuid || is_self_by_name))
                throw Exception(ErrorCodes::ACCESS_DENIED,
                    "User `{}` cannot revoke rights from themselves, even with PROTECTED_ACCESS_MANAGEMENT permission",
                    current_user_name);

            if (grantee_entity && grantee_entity->isProtected())
                requires_protected_priv = true;
        }
        if (requires_protected_priv)
            current_user_access->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
    }

    /// Collect access rights and roles we're going to grant or revoke.
    AccessRightsElements elements_to_grant;
    AccessRightsElements elements_to_revoke;
    AccessRightsElements elements_to_revoke_after_grant;
    collectAccessRightsElementsToGrantOrRevoke(query, elements_to_grant, elements_to_revoke, elements_to_revoke_after_grant);

    std::vector<UUID> roles_to_grant;
    RolesOrUsersSet roles_to_revoke;
    collectRolesToGrantOrRevoke(access_control, query, roles_to_grant, roles_to_revoke);

    /// Replacing empty database with the default. This step must be done before replication to avoid privilege escalation.
    String current_database = getContext()->getCurrentDatabase();
    elements_to_grant.replaceEmptyDatabase(current_database);
    elements_to_revoke.replaceEmptyDatabase(current_database);
    elements_to_revoke_after_grant.replaceEmptyDatabase(current_database);
    query.access_rights_elements.replaceEmptyDatabase(current_database);
    query.access_rights_elements_to_revoke.replaceEmptyDatabase(current_database);

    /// Executing on cluster.
    if (!query.cluster.empty())
    {
        if (query.current_grants)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "GRANT CURRENT GRANTS can't be executed on cluster.");

        auto required_access = getRequiredAccessForExecutingOnCluster(elements_to_grant, elements_to_revoke, elements_to_revoke_after_grant);
        checkAdminOptionForExecutingOnCluster(*current_user_access, roles_to_grant, roles_to_revoke);
        current_user_access->checkGranteesAreAllowed(grantees);
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(updated_query, getContext(), params);
    }

    /// Check if the current user has corresponding access rights granted with grant option.
    bool need_check_grantees_are_allowed = true;
    if (!query.current_grants)
        checkGrantOption(access_control, *current_user_access, grantees, need_check_grantees_are_allowed, elements_to_grant, elements_to_revoke, elements_to_revoke_after_grant);

    /// Check if the current user has corresponding roles granted with admin option.
    checkAdminOption(access_control, *current_user_access, grantees, need_check_grantees_are_allowed, roles_to_grant, roles_to_revoke, query.admin_option);

    if (need_check_grantees_are_allowed)
        current_user_access->checkGranteesAreAllowed(grantees);

    AccessRights new_rights;
    if (query.current_grants)
        calculateCurrentGrantRightsWithIntersection(new_rights, current_user_access, elements_to_grant);

    /// Update roles and users listed in `grantees`.
    auto update_func = [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
    {
        if (entity->isProtected())
            current_user_access->checkAccess(AccessFlags{AccessType::PROTECTED_ACCESS_MANAGEMENT});
        auto clone = entity->clone();
        if (query.current_grants)
            grantCurrentGrants(*clone, new_rights, elements_to_revoke, elements_to_revoke_after_grant);
        else
            updateGrantedAccessRightsAndRoles(*clone, elements_to_grant, elements_to_revoke, elements_to_revoke_after_grant, roles_to_grant, roles_to_revoke, query.admin_option);
        return clone;
    };

    access_control.update(grantees, update_func);

    return {};
}


void InterpreterGrantQuery::updateUserFromQuery(User & user, const ASTGrantQuery & query)
{
    updateFromQuery(user, query);
}

void InterpreterGrantQuery::updateRoleFromQuery(Role & role, const ASTGrantQuery & query)
{
    updateFromQuery(role, query);
}

void registerInterpreterGrantQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterGrantQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterGrantQuery", create_fn);
}

}
