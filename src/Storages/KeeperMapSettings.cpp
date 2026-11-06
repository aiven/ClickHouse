#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeeperMapSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

/** StorageKeeperMap table settings
  */
#define LIST_OF_KEEPER_MAP_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Bool, read_only, false, "Reject `INSERT`, `TRUNCATE` and mutations on this table. The data in Keeper stays readable, and other tables using the same path are not affected.", 0) \

DECLARE_SETTINGS_TRAITS(KeeperMapSettingsTraits, LIST_OF_KEEPER_MAP_SETTINGS, KEEPER_MAP_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(KeeperMapSettingsTraits, LIST_OF_KEEPER_MAP_SETTINGS, KeeperMapSettings, KeeperMapSetting)


KeeperMapSettings::KeeperMapSettings() : impl(std::make_unique<KeeperMapSettingsImpl>())
{
}

KeeperMapSettings::KeeperMapSettings(const KeeperMapSettings & settings) : impl(std::make_unique<KeeperMapSettingsImpl>(*settings.impl))
{
}

KeeperMapSettings::KeeperMapSettings(KeeperMapSettings && settings) noexcept = default;

KeeperMapSettings::~KeeperMapSettings() = default;

KEEPER_MAP_SETTINGS_SUPPORTED_TYPES(KeeperMapSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void KeeperMapSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

void KeeperMapSettings::loadFromQuery(const ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
}

bool KeeperMapSettings::hasBuiltin(std::string_view name)
{
    return KeeperMapSettingsImpl::hasBuiltin(name);
}

void KeeperMapSettings::checkCanSet(std::string_view name, const Field & value)
{
    KeeperMapSettingsImpl::checkCanSet(name, value);
}
}
