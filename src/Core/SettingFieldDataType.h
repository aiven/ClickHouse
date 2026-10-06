#pragma once

#include <Core/SettingsFields.h>
#include <DataTypes/IDataType_fwd.h>


namespace DB
{

/// Represents a data type, can be parsed from string (the name of the type),
/// outputs to string as the name of the type (so it can be parsed back).
/// NOTE(aiven): upstream master removed the `SettingFieldBase` polymorphic base; on 26.3 every
/// setting field type still derives from it, so this type is adapted to that contract.
struct SettingFieldDataType final : SettingFieldBase
{
    DataTypePtr value;
    bool changed = false;

    explicit SettingFieldDataType(const DataTypePtr & type = {});
    explicit SettingFieldDataType(const String & str);
    explicit SettingFieldDataType(const Field & f);

    /// NOTE(aiven): member-wise rather than `= default`, so the copy does not go through
    /// `SettingFieldBase`'s implicit copy constructor, which 26.3 deprecates because that
    /// base has a user-declared destructor (`-Wdeprecated-copy-with-dtor` is an error here).
    SettingFieldDataType(const SettingFieldDataType & o) : value(o.value), changed(o.changed) {}
    SettingFieldDataType & operator =(const SettingFieldDataType & o)
    {
        if (this != &o)
        {
            value = o.value;
            changed = o.changed;
        }
        return *this;
    }

    SettingFieldDataType & operator =(const DataTypePtr & type) { value = type; changed = true; return *this; }
    SettingFieldDataType & operator =(const String & str);
    SettingFieldDataType & operator =(const Field & f) override;

    bool isChanged() const override { return changed; }
    void setChanged(bool changed_) override { changed = changed_; }

    operator const DataTypePtr &() const { return value; } /// NOLINT
    explicit operator bool() const { return value != nullptr; }
    explicit operator Field() const override { return toString(); }

    String toString() const override;
    void parseFromString(const String & str) override;

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
};

}
