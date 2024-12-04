#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <azure/core/http/http.hpp>
#include <azure/storage/common/internal/shared_key_policy.hpp>

namespace DB
{

class AzureDelegatedKeyPolicy : public Azure::Storage::_internal::SharedKeyPolicy
{
public:
    explicit AzureDelegatedKeyPolicy(const std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> &credential, const std::string & signature_delegation_url_)
        : SharedKeyPolicy(credential)
        , signature_delegation_url(signature_delegation_url_)
    {
    }

    ~AzureDelegatedKeyPolicy() override = default;

    std::unique_ptr<HttpPolicy> Clone() const override
    {
        return std::make_unique<AzureDelegatedKeyPolicy>(m_credential, signature_delegation_url);
    }

protected:
    std::string GetSignature(const std::string& string_to_sign) const override;

    std::string signature_delegation_url;
};

}

#endif
