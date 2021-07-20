#pragma once

#include <Access/IAccessEntity.h>

namespace DB
{

String serializeAccessEntity(const IAccessEntity & entity);

AccessEntityPtr parseAccessEntity(const String & definition, const String & source_type, const String & source);

}
