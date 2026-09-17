#include <Parsers/ASTViewTargets.h>

#include <Common/quoteString.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/CommonParsers.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace
{
    Keyword getKeyword(ViewTarget::Kind kind, std::optional<UInt64> time_series_version = {})
    {
        if ((kind == ViewTarget::MetricFamilies) && time_series_version && (*time_series_version < TimeSeriesVersion::MIN_WITH_METRIC_FAMILIES_TARGET_NAME))
            return Keyword::METRICS;

        switch (kind)
        {
            case ViewTarget::To:      return Keyword::TO;      /// TO mydb.mysamples
            case ViewTarget::Inner:   return Keyword::INNER;   /// INNER ENGINE = MergeTree()
            case ViewTarget::Samples: return Keyword::SAMPLES; /// SAMPLES mydb.mysamples
            case ViewTarget::RecentSamples: return Keyword::RECENT_SAMPLES; /// RECENT SAMPLES mydb.myrecentsamples
            case ViewTarget::Tags:    return Keyword::TAGS;    /// TAGS mydb.mytags
            case ViewTarget::MetricFamilies: return Keyword::METRIC_FAMILIES; /// METRIC FAMILIES mydb.mymetricfamilies
        }
        UNREACHABLE();
    }
}

ViewTarget::~ViewTarget() = default;
ViewTarget::ViewTarget() = default;
ViewTarget::ViewTarget(const ViewTarget & other) = default;
ViewTarget & ViewTarget::operator=(const ViewTarget & other) = default;

ViewTarget::ViewTarget(Kind kind_) : kind(kind_) {}

std::vector<ViewTarget::Kind> ASTViewTargets::getKinds() const
{
    std::vector<ViewTarget::Kind> kinds;
    kinds.reserve(targets.size());
    for (const auto & target : targets)
        kinds.push_back(target.kind);
    return kinds;
}


void ASTViewTargets::setTableID(ViewTarget::Kind kind, const StorageID & table_id_)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            target.table_id = table_id_;
            return;
        }
    }
    if (table_id_)
        targets.emplace_back(kind).table_id = table_id_;
}

StorageID ASTViewTargets::getTableID(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->table_id;
    return StorageID::createEmpty();
}

bool ASTViewTargets::hasTableID(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return !target->table_id.empty();
    return false;
}

void ASTViewTargets::setCurrentDatabase(const String & current_database)
{
    for (auto & target : targets)
    {
        auto & table_id = target.table_id;
        if (!table_id.table_name.empty() && table_id.database_name.empty())
            table_id.database_name = current_database;
    }
}

void ASTViewTargets::setInnerUUID(ViewTarget::Kind kind, const UUID & inner_uuid_)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            target.inner_uuid = inner_uuid_;
            return;
        }
    }
    if (inner_uuid_ != UUIDHelpers::Nil)
        targets.emplace_back(kind).inner_uuid = inner_uuid_;
}

UUID ASTViewTargets::getInnerUUID(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind))
        return target->inner_uuid;
    return UUIDHelpers::Nil;
}

bool ASTViewTargets::hasInnerUUID(ViewTarget::Kind kind) const
{
    return getInnerUUID(kind) != UUIDHelpers::Nil;
}

void ASTViewTargets::resetInnerUUIDs()
{
    for (auto & target : targets)
        target.inner_uuid = UUIDHelpers::Nil;
}

bool ASTViewTargets::hasInnerUUIDs() const
{
    for (const auto & target : targets)
    {
        if (target.inner_uuid != UUIDHelpers::Nil)
            return true;
    }
    return false;
}

void ASTViewTargets::setInnerEngine(ViewTarget::Kind kind, ASTPtr new_inner_engine)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            if (target.inner_engine == new_inner_engine)
                return;
            if (new_inner_engine)
                setOrReplace(target.inner_engine, std::move(new_inner_engine));
            else
                reset(target.inner_engine);
            return;
        }
    }

    if (new_inner_engine)
    {
        auto & new_target = targets.emplace_back(kind);
        set(new_target.inner_engine, std::move(new_inner_engine));
    }
}

ASTStorage * ASTViewTargets::getInnerEngine(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind); target && target->inner_engine)
        return target->inner_engine->as<ASTStorage>();
    return nullptr;
}

std::vector<ASTStorage *> ASTViewTargets::getInnerEngines() const
{
    std::vector<ASTStorage *> res;
    res.reserve(targets.size());
    for (const auto & target : targets)
    {
        if (target.inner_engine)
            res.push_back(target.inner_engine->as<ASTStorage>());
    }
    return res;
}

void ASTViewTargets::setInnerColumns(ViewTarget::Kind kind, ASTPtr new_inner_columns)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            if (target.inner_columns == new_inner_columns)
                return;
            if (new_inner_columns)
                setOrReplace(target.inner_columns, std::move(new_inner_columns));
            else
                reset(target.inner_columns);
            return;
        }
    }

    if (new_inner_columns)
    {
        auto & new_target = targets.emplace_back(kind);
        set(new_target.inner_columns, std::move(new_inner_columns));
    }
}

ASTColumns * ASTViewTargets::getInnerColumns(ViewTarget::Kind kind) const
{
    if (const auto * target = tryGetTarget(kind); target && target->inner_columns)
        return target->inner_columns->as<ASTColumns>();
    return nullptr;
}

const ViewTarget * ASTViewTargets::tryGetTarget(ViewTarget::Kind kind) const
{
    for (const auto & target : targets)
    {
        if (target.kind == kind)
            return &target;
    }
    return nullptr;
}

void ASTViewTargets::removeTarget(ViewTarget::Kind kind)
{
    for (auto it = targets.begin(); it != targets.end(); ++it)
    {
        if (it->kind == kind)
        {
            /// Detach the target's ASTs from `children` before erasing the target.
            reset(it->inner_engine);
            reset(it->inner_columns);
            reset(it->table_ast);
            targets.erase(it);
            return;
        }
    }
}

ASTPtr ASTViewTargets::clone() const
{
    auto res = make_intrusive<ASTViewTargets>(*this);
    res->children.clear();
    for (auto & target : res->targets)
    {
        if (target.inner_engine)
            res->set(target.inner_engine, target.inner_engine->clone());
        if (target.inner_columns)
            res->set(target.inner_columns, target.inner_columns->clone());
        if (target.table_ast)
            res->set(target.table_ast, target.table_ast->clone());
    }
    return res;
}

void ASTViewTargets::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : targets)
        formatTarget(target, ostr, s, state, frame);
}

void ASTViewTargets::formatTarget(ViewTarget::Kind kind, WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame, std::optional<UInt64> time_series_version) const
{
    for (const auto & target : targets)
    {
        if (target.kind == kind)
            formatTarget(target, ostr, s, state, frame, time_series_version);
    }
}

void ASTViewTargets::formatTarget(const ViewTarget & target, WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame, std::optional<UInt64> time_series_version)
{
    if (target.table_id)
    {
        ostr << s.nl_or_ws << toStringView(getKeyword(target.kind, time_series_version)) << " "
             << (!target.table_id.database_name.empty() ? backQuoteIfNeed(target.table_id.database_name) + "." : "")
             << backQuoteIfNeed(target.table_id.table_name);
    }

    if (target.inner_uuid != UUIDHelpers::Nil)
    {
        ostr << s.nl_or_ws;
        /// Skip the "kind" prefix for ViewTarget::Inner to avoid producing "INNER INNER UUID".
        if (target.kind != ViewTarget::Inner)
            ostr << toStringView(getKeyword(target.kind, time_series_version)) << " ";
        ostr << "INNER UUID " << quoteString(toString(target.inner_uuid));
    }

    if (target.inner_columns)
    {
        ostr << s.nl_or_ws;
        if (target.kind != ViewTarget::Inner)
            ostr << toStringView(getKeyword(target.kind, time_series_version)) << " ";
        ostr << "INNER COLUMNS" << s.nl_or_ws << "(";
        auto inner_frame = frame;
        inner_frame.expression_list_always_start_on_new_line = true;
        target.inner_columns->format(ostr, s, state, inner_frame);
        if (!s.one_line)
            ostr << "\n";
        ostr << ")";
    }

    if (target.inner_engine)
    {
        /// Skip both the "kind" and "INNER" prefixes for ViewTarget::To to produce just "ENGINE" (and not "TO INNER ENGINE").
        if (target.kind != ViewTarget::To)
        {
            ostr << s.nl_or_ws;
            /// Skip the "kind" prefix for ViewTarget::Inner to avoid producing "INNER INNER ENGINE".
            if (target.kind != ViewTarget::Inner)
                ostr << toStringView(getKeyword(target.kind, time_series_version)) << " ";
            ostr << "INNER";
        }
        target.inner_engine->format(ostr, s, state, frame);
    }
}

void ASTViewTargets::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    for (auto & target : targets)
    {
        f(nullptr, &target.table_ast);
        f(nullptr, &target.inner_engine);
        f(nullptr, &target.inner_columns);
    }
}

void ASTViewTargets::setTableASTWithQueryParams(ViewTarget::Kind kind, ASTPtr new_table_ast)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            if (target.table_ast == new_table_ast)
                return;
            if (new_table_ast)
                setOrReplace(target.table_ast, std::move(new_table_ast));
            else
                reset(target.table_ast);
            return;
        }
    }

    if (new_table_ast)
    {
        auto & new_target = targets.emplace_back(kind);
        set(new_target.table_ast, std::move(new_table_ast));
    }
}

bool ASTViewTargets::hasTableASTWithQueryParams(ViewTarget::Kind kind) const
{
    for (const auto & target : targets)
        if (target.kind == kind)
            return target.table_ast != nullptr;
    return false;
}

ASTPtr ASTViewTargets::getTableASTWithQueryParams(ViewTarget::Kind kind)
{
    for (const auto & target : targets)
        if (target.kind == kind)
            return target.table_ast;
    return nullptr;
}

}
