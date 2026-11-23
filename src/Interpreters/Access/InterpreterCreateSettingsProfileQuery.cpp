#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCreateSettingsProfileQuery.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/SettingsProfile.h>
#include <Core/Settings.h>
#include <Interpreters/Access/checkProtectedTargets.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_non_default_profile;
}

namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int SETTING_CONSTRAINT_VIOLATION;
}

namespace
{
    void updateSettingsProfileFromQueryImpl(
        SettingsProfile & profile,
        const ASTCreateSettingsProfileQuery & query,
        const String & override_name,
        const std::optional<AlterSettingsProfileElements> & override_settings,
        const std::optional<RolesOrUsersSet> & override_to_roles)
    {
        if (!override_name.empty())
            profile.setName(override_name);
        else if (!query.new_name.empty())
            profile.setName(query.new_name);
        else if (query.names.size() == 1)
            profile.setName(query.names.front());

        if (override_settings)
            profile.elements.applyChanges(*override_settings);
        else if (query.alter_settings)
            profile.elements.applyChanges(AlterSettingsProfileElements{*query.alter_settings});
        else if (query.settings)
            profile.elements.applyChanges(AlterSettingsProfileElements{*query.settings});

        if (override_to_roles)
            profile.to_roles = *override_to_roles;
        else if (query.to_roles)
            profile.to_roles = *query.to_roles;
    }
}


BlockIO InterpreterCreateSettingsProfileQuery::execute()
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query_ptr->as<ASTCreateSettingsProfileQuery &>();

    auto & access_control = getContext()->getAccessControl();

    /// `CREATE SETTINGS PROFILE OR REPLACE` throws away an existing profile of the same name - including
    /// which roles it applies to - so it is a drop followed by a create and requires the privileges of
    /// both. `DROP SETTINGS PROFILE` is required whether or not the profile currently exists, mirroring
    /// `REPLACE TABLE`, so that the check does not reveal which profiles exist either.
    AccessFlags required_access = query.alter ? AccessType::ALTER_SETTINGS_PROFILE : AccessType::CREATE_SETTINGS_PROFILE;
    if (query.or_replace)
        required_access |= AccessType::DROP_SETTINGS_PROFILE;

    getContext()->checkAccess(required_access);

    std::vector<UUID> name_ids;
    if (query.alter)
    {
        if (query.if_exists)
        {
            name_ids = access_control.find<SettingsProfile>(query.names);
        }
        else
        {
            name_ids = access_control.getIDs<SettingsProfile>(query.names);
        }
    }

    std::optional<AlterSettingsProfileElements> settings_from_query;
    if (query.alter_settings)
        settings_from_query = AlterSettingsProfileElements{*query.alter_settings, access_control};
    else if (query.settings)
        settings_from_query = AlterSettingsProfileElements{SettingsProfileElements(*query.settings, access_control)};

    if (settings_from_query)
    {
        for (const auto & element : settings_from_query->add_settings)
        {
            if (element.parent_profile.has_value())
            {
                for (const auto & name_id: name_ids)
                {
                    if (access_control.isExpectedProfileOrDescendant(element.parent_profile.value(), name_id))
                    {
                        throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Inherited profiles may not have circular dependencies");
                    }
                }
            }
        }
        /// With `allow_non_default_profile = 0` this also requires each parent to be the default profile or to inherit from it.
        if (!query.attach)
            getContext()->checkSettingsConstraints(*settings_from_query, SettingSource::PROFILE);
    }

    /// A created profile, or an altered one whose settings are rewritten, must have the default profile as an ancestor.
    /// A profile left without any parent gets the default profile as its parent instead of being rejected.
    /// An `ALTER` without settings (a no-op or a rename) keeps the parents the profile already has.
    std::optional<UUID> default_parent_to_inject;
    if ((!query.alter || settings_from_query) && !getContext()->getSettingsRef()[Setting::allow_non_default_profile])
    {
        const auto & default_profile_id = access_control.getDefaultProfileId();
        if (!default_profile_id.has_value())
            throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Cannot alter or create profiles if default profile is not configured.");
        bool has_default_profile = false;
        for (const auto & name_id : name_ids)
            if (name_id == default_profile_id.value())
                has_default_profile = true;
        if (has_default_profile && query.names.size() > 1)
            throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Cannot alter the default and other profiles simultaneously.");
        default_parent_to_inject = default_profile_id;
    }

    /// Decided on the profile as written, so that an `ALTER` keeps the parents the profile has. The default profile goes
    /// first: later parents override earlier ones, and it must not override a stricter parent.
    auto inject_default_parent = [&](SettingsProfile & profile, const std::optional<UUID> & profile_id)
    {
        if (!default_parent_to_inject || (profile_id == default_parent_to_inject))
            return;
        for (const auto & element : profile.elements)
        {
            if (element.parent_profile)
                return;
        }
        SettingsProfileElement default_parent;
        default_parent.parent_profile = default_parent_to_inject;
        profile.elements.insert(profile.elements.begin(), std::move(default_parent));
    };

    /// Aiven patch 022: the `TO <grantee>` clause does not rewrite the listed entities - the
    /// set is stored on this profile - but it decides whom the profile applies to, so aiming
    /// one at a protected user neuters it without touching its definition. Checked before the
    /// ON CLUSTER dispatch below, which would otherwise replay under another identity.
    if (query.to_roles)
        checkProtectedTargets(getContext(), *query.to_roles);

    if (query.alter || query.or_replace)
    {
        const auto existing_ids = access_control.find<SettingsProfile>(query.names);
        for (const auto & id : existing_ids)
            if (auto existing = access_control.tryRead<SettingsProfile>(id))
                checkProtectedTargets(getContext(), existing->to_roles);

        /// Altering a profile changes what applies to every entity inheriting it. The replacement gets a new ID,
        /// so the old one is removed and every entity that held it loses it, exactly as with `DROP SETTINGS PROFILE`.
        if (query.alter)
            checkNoProtectedHolders(getContext(), existing_ids);
        else
            checkNoProtectedDependents(getContext(), existing_ids);
    }

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTag(getContext()->getUserName());
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());
    }

    std::optional<RolesOrUsersSet> roles_from_query;
    if (query.to_roles)
        roles_from_query = RolesOrUsersSet{*query.to_roles, access_control, getContext()->getUserID()};


    IAccessStorage * storage = &access_control;
    MultipleAccessStorage::StoragePtr storage_ptr;

    if (!query.storage_name.empty())
    {
        storage_ptr = access_control.getStorageByName(query.storage_name);
        storage = storage_ptr.get();
    }

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity, const UUID & id) -> AccessEntityPtr
        {
            auto updated_profile = typeid_cast<std::shared_ptr<SettingsProfile>>(entity->clone());
            updateSettingsProfileFromQueryImpl(*updated_profile, query, {}, settings_from_query, roles_from_query);
            inject_default_parent(*updated_profile, id);
            return updated_profile;
        };
        if (query.if_exists)
        {
            auto ids = storage->find<SettingsProfile>(query.names);
            storage->tryUpdate(ids, update_func);
        }
        else
            storage->update(storage->getIDs<SettingsProfile>(query.names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_profiles;
        for (const auto & name : query.names)
        {
            auto new_profile = std::make_shared<SettingsProfile>();
            updateSettingsProfileFromQueryImpl(*new_profile, query, name, settings_from_query, roles_from_query);
            inject_default_parent(*new_profile, {});
            new_profiles.emplace_back(std::move(new_profile));
        }

        if (!query.storage_name.empty())
        {
            for (const auto & name : query.names)
            {
                if (auto another_storage_ptr = access_control.findExcludingStorage(AccessEntityType::SETTINGS_PROFILE, name, storage_ptr))
                    throw Exception(ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, "Settings profile {} already exists in storage {}", name, another_storage_ptr->getStorageName());
            }
        }

        if (query.if_not_exists)
            storage->tryInsert(new_profiles);
        else if (query.or_replace)
            storage->insertOrReplace(new_profiles);
        else
            storage->insert(new_profiles);
    }

    return {};
}


void InterpreterCreateSettingsProfileQuery::updateSettingsProfileFromQuery(SettingsProfile & SettingsProfile, const ASTCreateSettingsProfileQuery & query)
{
    updateSettingsProfileFromQueryImpl(SettingsProfile, query, {}, {}, {});
}

void registerInterpreterCreateSettingsProfileQuery(InterpreterFactory & factory);
void registerInterpreterCreateSettingsProfileQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateSettingsProfileQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateSettingsProfileQuery", create_fn);
}

}
