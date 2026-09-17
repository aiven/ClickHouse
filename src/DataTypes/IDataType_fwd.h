#pragma once

#include <memory>
#include <vector>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
/// NOTE(aiven): upstream master uses VectorWithMemoryTracking for both aliases below.
/// On 26.3 these are plain std::vector everywhere, so the forward-declaration header
/// keeps the base version's convention; switching them is a separate upstream change.
using DataTypes = std::vector<DataTypePtr>;

struct DataTypeWithConstInfo
{
    DataTypePtr type;
    bool is_const;
};

using DataTypesWithConstInfo = std::vector<DataTypeWithConstInfo>;

}
