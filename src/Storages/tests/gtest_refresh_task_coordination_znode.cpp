#include <gtest/gtest.h>

#include <Storages/MaterializedView/RefreshTask.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>

#include <cstring>

using namespace DB;

namespace
{

using Znode = RefreshTask::CoordinationZnode;

const LoggerPtr & testLogger()
{
    static LoggerPtr log = getLogger("gtest_refresh_task_coordination_znode");
    return log;
}

Znode parseZnode(const String & data, bool running_znode_exists = false)
{
    Znode znode;
    znode.parse(data, running_znode_exists, testLogger());
    return znode;
}

/// Legacy order: `refresh_dir` and `target_table_id` before `randomness`, no `refresh_running`.
const String znode_legacy =
    "format version: 1\n"
    "last_completed_timeslot: 1700000000\n"
    "last_success_time: 1700000060\n"
    "last_success_duration_ms: 64\n"
    "last_success_table_uuid: 11111111-2222-3333-4444-555555555555\n"
    "last_attempt_time: 1700000061\n"
    "last_attempt_replica: node-1.example\n"
    "last_attempt_error: \n"
    "last_attempt_succeeded: 1\n"
    "previous_attempt_error: \n"
    "attempt_number: 0\n"
    "refresh_dir: _refresh_1700000060000\n"
    "target_table_id: \n"
    "randomness: -400509369\n";

/// Current order, with the obsolete `target_table_id`.
const String znode_current =
    "format version: 1\n"
    "last_completed_timeslot: 1700000000\n"
    "last_success_time: 1700000060\n"
    "last_success_duration_ms: 64\n"
    "last_success_table_uuid: 11111111-2222-3333-4444-555555555555\n"
    "last_attempt_time: 1700000061\n"
    "last_attempt_replica: shard1/node-1.example\n"
    "last_attempt_error: \n"
    "last_attempt_succeeded: 1\n"
    "previous_attempt_error: \n"
    "attempt_number: 0\n"
    "randomness: -400509369\n"
    "refresh_running: 1\n"
    "refresh_dir: _refresh_1700000060000\n"
    "target_table_id: \n";

}

TEST(RefreshTaskCoordinationZnode, ParsesLegacyLayout)
{
    Znode znode = parseZnode(znode_legacy);

    EXPECT_EQ(znode.last_completed_timeslot.time_since_epoch().count(), 1700000000);
    EXPECT_EQ(znode.last_success_time.time_since_epoch().count(), 1700000060);
    EXPECT_EQ(znode.last_success_duration.count(), 64);
    EXPECT_EQ(toString(znode.last_success_table_uuid), "11111111-2222-3333-4444-555555555555");
    EXPECT_EQ(znode.last_attempt_time.time_since_epoch().count(), 1700000061);
    EXPECT_EQ(znode.last_attempt_replica, "node-1.example");
    EXPECT_TRUE(znode.last_attempt_succeeded);
    EXPECT_EQ(znode.attempt_number, 0);
    EXPECT_EQ(znode.randomness, -400509369);
    EXPECT_EQ(znode.refresh_dir, "_refresh_1700000060000");
    /// No `refresh_running` field: falls back to whether the "running" znode exists.
    EXPECT_FALSE(znode.refresh_running);
    EXPECT_TRUE(parseZnode(znode_legacy, /*running_znode_exists=*/ true).refresh_running);
}

TEST(RefreshTaskCoordinationZnode, ParsesCurrentLayout)
{
    Znode znode = parseZnode(znode_current);

    EXPECT_EQ(znode.last_attempt_replica, "shard1/node-1.example");
    EXPECT_EQ(znode.randomness, -400509369);
    EXPECT_TRUE(znode.refresh_running);
    EXPECT_EQ(znode.refresh_dir, "_refresh_1700000060000");
}

TEST(RefreshTaskCoordinationZnode, RewritesLegacyLayoutInCurrentOrder)
{
    /// A legacy znode is written back in the current order, without `target_table_id`.
    String expected = znode_current;
    expected.replace(expected.find("shard1/"), strlen("shard1/"), "");
    expected.replace(expected.find("refresh_running: 1"), strlen("refresh_running: 1"), "refresh_running: 0");
    expected.erase(expected.find("target_table_id: \n"));

    EXPECT_EQ(parseZnode(znode_legacy).toString(), expected);
}

TEST(RefreshTaskCoordinationZnode, RoundTripsEscapedStrings)
{
    Znode znode = parseZnode(znode_current);
    znode.last_attempt_error = "Code: 1. line 1\nline 2: with colon\tand tab";
    znode.previous_attempt_error = "previous\nerror";
    znode.attempt_number = 3;
    znode.refresh_running = false;

    Znode parsed = parseZnode(znode.toString(), /*running_znode_exists=*/ true);
    EXPECT_EQ(parsed.last_attempt_error, znode.last_attempt_error);
    EXPECT_EQ(parsed.previous_attempt_error, znode.previous_attempt_error);
    EXPECT_EQ(parsed.attempt_number, 3);
    EXPECT_FALSE(parsed.refresh_running);
    EXPECT_EQ(parsed.toString(), znode.toString());
}

TEST(RefreshTaskCoordinationZnode, IgnoresUnknownFields)
{
    /// E.g. fields added by newer versions.
    String data = znode_current + "last_success_end_time_ns: 1700000060064000000\ncursor: \n";
    EXPECT_EQ(parseZnode(data).toString(), parseZnode(znode_current).toString());
}

TEST(RefreshTaskCoordinationZnode, RejectsMissingRequiredField)
{
    String data = znode_legacy;
    data.erase(data.find("randomness: "));

    Znode znode = parseZnode(znode_current);
    String before = znode.toString();
    EXPECT_THROW(znode.parse(data, false, testLogger()), Exception);
    /// A failed parse leaves the previous state intact.
    EXPECT_EQ(znode.toString(), before);
}

TEST(RefreshTaskCoordinationZnode, RejectsDuplicateField)
{
    EXPECT_THROW(parseZnode(znode_current + "attempt_number: 1\n"), Exception);
}
