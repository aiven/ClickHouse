#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Core/SettingsSecrets.h>
#include <optional>

namespace Kafka
{

static constexpr auto TABLE_ENGINE_NAME = "Kafka";
static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &){ return "'[HIDDEN]'"; };

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"kafka_sasl_password", DEFAULT_MASKING_RULE},
    /// Aiven alias of `format_avro_schema_registry_url`, masked the same way: only the URL's credentials.
    {"kafka_format_avro_schema_registry_url",
     [](const DB::Field & value) { return DB::CoreSettings::renderSecretSettingValue("format_avro_schema_registry_url", value); }},
};

}
