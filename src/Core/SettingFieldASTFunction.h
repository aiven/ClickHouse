#pragma once

#include <Core/SettingsFields.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

/// Represents ASTFunction, can be parsed from string,
/// outputs to string with secrets (so it can be parsed back).
/// NOTE(aiven): upstream master removed the `SettingFieldBase` polymorphic base; on 26.3 every
/// setting field type still derives from it, so this type is adapted to that contract.
struct SettingFieldASTFunction final : SettingFieldBase
{
    boost::intrusive_ptr<ASTFunction> value;
    bool changed = false;

    explicit SettingFieldASTFunction(const boost::intrusive_ptr<ASTFunction> & func = {});
    explicit SettingFieldASTFunction(const String & str);
    explicit SettingFieldASTFunction(const Field & f);

    /// NOTE(aiven): member-wise rather than `= default`, so the copy does not go through
    /// `SettingFieldBase`'s implicit copy constructor, which 26.3 deprecates because that
    /// base has a user-declared destructor (`-Wdeprecated-copy-with-dtor` is an error here).
    SettingFieldASTFunction(const SettingFieldASTFunction & o) : value(o.value), changed(o.changed) {}
    SettingFieldASTFunction & operator =(const SettingFieldASTFunction & o)
    {
        if (this != &o)
        {
            value = o.value;
            changed = o.changed;
        }
        return *this;
    }

    SettingFieldASTFunction & operator =(const boost::intrusive_ptr<ASTFunction> & func) { value = func; changed = true; return *this; }
    SettingFieldASTFunction & operator =(const String & str);
    SettingFieldASTFunction & operator =(const Field & f) override;

    bool isChanged() const override { return changed; }
    void setChanged(bool changed_) override { changed = changed_; }

    operator const boost::intrusive_ptr<ASTFunction> &() const { return value; } /// NOLINT
    explicit operator bool() const { return value != nullptr; }
    explicit operator Field() const override { return toString(); }

    String toString() const override;
    void parseFromString(const String & str) override;

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
};

}
