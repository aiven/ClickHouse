#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

namespace DB
{
class ASTStorage;
struct KeeperMapSettingsImpl;
class SettingsChanges;

/// List of available types supported in KeeperMapSettings object
#define KEEPER_MAP_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool)

KEEPER_MAP_SETTINGS_SUPPORTED_TYPES(KeeperMapSettings, DECLARE_SETTING_TRAIT)

struct KeeperMapSettings
{
    KeeperMapSettings();
    KeeperMapSettings(const KeeperMapSettings & settings);
    KeeperMapSettings(KeeperMapSettings && settings) noexcept;
    ~KeeperMapSettings();

    KEEPER_MAP_SETTINGS_SUPPORTED_TYPES(KeeperMapSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void applyChanges(const SettingsChanges & changes);
    void loadFromQuery(const ASTStorage & storage_def);

    static bool hasBuiltin(std::string_view name);
    static void checkCanSet(std::string_view name, const Field & value);

private:
    std::unique_ptr<KeeperMapSettingsImpl> impl;
};
}
