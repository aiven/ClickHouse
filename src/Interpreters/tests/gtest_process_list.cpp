#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>

using namespace DB;

namespace
{

ContextMutablePtr makeQueryContextWithId(const String & query_id)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    context->setCurrentQueryId(query_id);
    return context;
}

}

/// Regression test for QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING raised by internal sub-queries.
///
/// Background: the process-list registration maps are keyed by `current_query_id` and require a
/// globally-unique id. Since the `!internal` insert gate was dropped, internal queries are registered
/// too, so an internal sub-query that inherits the still-registered id of the parent that spawned it
/// would either be rejected (duplicate-id guard) or — if the guard were merely skipped — desync the
/// maps and `std::terminate` on teardown. `ProcessList::insert` therefore mints a fresh id for an
/// internal query whose id is already in use, keeping uniqueness intact without an exception.
TEST(ProcessList, InternalQueryDuplicateIdIsRegeneratedNotRejected)
{
    ProcessList process_list;

    /// Parent internal query registers under a fixed id.
    auto parent_context = makeQueryContextWithId("shared_query_id");
    ProcessList::EntryPtr parent_entry;
    ASSERT_NO_THROW(parent_entry = process_list.insert("SELECT 'parent'", 0, nullptr, parent_context, 0, /*is_internal=*/ true));
    ASSERT_EQ(parent_context->getClientInfo().current_query_id, "shared_query_id");

    /// Child internal query inherits the SAME id while the parent is still registered.
    /// It must be admitted (no QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING) with a freshly-minted distinct id.
    auto child_context = makeQueryContextWithId("shared_query_id");
    ProcessList::EntryPtr child_entry;
    ASSERT_NO_THROW(child_entry = process_list.insert("SELECT 'child'", 0, nullptr, child_context, 0, /*is_internal=*/ true));

    EXPECT_NE(child_context->getClientInfo().current_query_id, "shared_query_id");
    EXPECT_FALSE(child_context->getClientInfo().current_query_id.empty());
    EXPECT_EQ(process_list.size(), 2u);

    /// Tearing down both entries exercises ~ProcessListEntry for each id; it must not std::terminate.
    child_entry.reset();
    parent_entry.reset();
    EXPECT_EQ(process_list.size(), 0u);
}

/// Mirrors `InterpreterCreateQuery::createReplicatedDatabaseByClient`: a parent internal query
/// (the inner CREATE) keeps its process-list entry alive (as `BlockIO::process_list_entries` does)
/// while a sibling internal query (the inner GRANT) reuses the SAME context and id. The sibling
/// must regenerate, AND tearing down the still-registered parent must not `std::terminate` even
/// though the shared context's id was re-stamped after the parent was inserted — this is safe only
/// because `QueryStatus` snapshots `client_info` at insert time.
TEST(ProcessList, InternalSiblingsSharingContextTearDownSafely)
{
    ProcessList process_list;

    auto shared_context = makeQueryContextWithId("shared_ctx_id");

    ProcessList::EntryPtr create_entry;
    ASSERT_NO_THROW(create_entry = process_list.insert("CREATE DATABASE d", 0, nullptr, shared_context, 0, /*is_internal=*/ true));
    ASSERT_EQ(shared_context->getClientInfo().current_query_id, "shared_ctx_id");

    /// Sibling reuses the same context (still id "shared_ctx_id") while the parent entry is alive.
    ProcessList::EntryPtr grant_entry;
    ASSERT_NO_THROW(grant_entry = process_list.insert("GRANT ... ON d.*", 0, nullptr, shared_context, 0, /*is_internal=*/ true));
    EXPECT_NE(shared_context->getClientInfo().current_query_id, "shared_ctx_id");
    EXPECT_EQ(process_list.size(), 2u);

    /// Tear down the parent FIRST: it must still resolve under its snapshot id "shared_ctx_id".
    create_entry.reset();
    grant_entry.reset();
    EXPECT_EQ(process_list.size(), 0u);
}

/// An internal query whose id does NOT collide keeps it, so logical correlation is preserved when
/// there is no conflict (regeneration fires only on an actual collision).
TEST(ProcessList, InternalQueryUniqueIdIsPreserved)
{
    ProcessList process_list;

    auto context = makeQueryContextWithId("unique_query_id");
    ProcessList::EntryPtr entry;
    ASSERT_NO_THROW(entry = process_list.insert("SELECT 1", 0, nullptr, context, 0, /*is_internal=*/ true));

    EXPECT_EQ(context->getClientInfo().current_query_id, "unique_query_id");
    EXPECT_EQ(process_list.size(), 1u);

    entry.reset();
    EXPECT_EQ(process_list.size(), 0u);
}
