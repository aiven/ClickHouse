#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    void formatCurrentGrantsElements(const AccessRightsElements & elements, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        ostr << "(";
        elements.formatElementsWithoutOptions(ostr, settings.hilite);
        ostr << ")";
    }
}


String ASTGrantQuery::getID(char) const
{
    return "GrantQuery";
}


ASTPtr ASTGrantQuery::clone() const
{
    auto res = std::make_shared<ASTGrantQuery>(*this);

    if (roles)
        res->roles = std::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    if (grantees)
        res->grantees = std::static_pointer_cast<ASTRolesOrUsersSet>(grantees->clone());

    return res;
}


void ASTGrantQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << (settings.hilite ? IAST::hilite_keyword : "") << (attach_mode ? "ATTACH " : "")
                  << (settings.hilite ? hilite_keyword : "") << (is_revoke ? "REVOKE" : "GRANT")
                  << (settings.hilite ? IAST::hilite_none : "");

    if (!access_rights_elements.sameOptions())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Elements of an ASTGrantQuery are expected to have the same options");
    if (!access_rights_elements.empty() &&  access_rights_elements[0].is_partial_revoke && !is_revoke)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A partial revoke should be revoked, not granted");
    if (!access_rights_elements_to_revoke.empty() && !access_rights_elements_to_revoke.sameOptions())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Revoke elements of an ASTGrantQuery are expected to have the same options");
    bool grant_option = !access_rights_elements.empty() && access_rights_elements[0].grant_option;

    formatOnCluster(ostr, settings);

    if (is_revoke)
    {
        if (grant_option)
            ostr << (settings.hilite ? hilite_keyword : "") << " GRANT OPTION FOR" << (settings.hilite ? hilite_none : "");
        else if (admin_option)
            ostr << (settings.hilite ? hilite_keyword : "") << " ADMIN OPTION FOR" << (settings.hilite ? hilite_none : "");
    }

    ostr << " ";
    if (roles)
    {
        roles->format(ostr, settings);
        if (!access_rights_elements.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "ASTGrantQuery can contain either roles or access rights elements "
                            "to grant or revoke, not both of them");
    }
    else if (current_grants)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "CURRENT GRANTS" << (settings.hilite ? hilite_none : "");
        formatCurrentGrantsElements(access_rights_elements, ostr, settings);
    }
    else
    {
        access_rights_elements.formatElementsWithoutOptions(ostr, settings.hilite);
    }

    /// Format the embedded EXCEPT clause (combined GRANT ... EXCEPT ... TO ... syntax)
    if (!access_rights_elements_to_revoke.empty())
    {
        ostr << " " << (settings.hilite ? hilite_keyword : "") << "EXCEPT" << (settings.hilite ? hilite_none : "") << " ";
        access_rights_elements_to_revoke.formatElementsWithoutOptions(ostr, settings.hilite);
    }

    ostr << (settings.hilite ? IAST::hilite_keyword : "") << (is_revoke ? " FROM " : " TO ")
                  << (settings.hilite ? IAST::hilite_none : "");
    grantees->format(ostr, settings);

    if (!is_revoke)
    {
        if (grant_option)
            ostr << (settings.hilite ? hilite_keyword : "") << " WITH GRANT OPTION" << (settings.hilite ? hilite_none : "");
        else if (admin_option)
            ostr << (settings.hilite ? hilite_keyword : "") << " WITH ADMIN OPTION" << (settings.hilite ? hilite_none : "");

        if (replace_access || replace_granted_roles)
            ostr << (settings.hilite ? hilite_keyword : "") << " WITH REPLACE OPTION" << (settings.hilite ? hilite_none : "");
    }
}


void ASTGrantQuery::replaceEmptyDatabase(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
    access_rights_elements_to_revoke.replaceEmptyDatabase(current_database);
}


void ASTGrantQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (grantees)
        grantees->replaceCurrentUserTag(current_user_name);
}

}
