#pragma once

#include <mutex>
#include <Poco/Channel.h>
#include <Poco/Message.h>
#include <Loggers/ExtendedLogMessage.h>

namespace DB
{

/** A channel that sends logs to systemd journal using the native protocol.
  *
  * This is a simple channel like Poco::ConsoleChannel - it receives already-formatted
  * messages and sends them to the journal. Wrap with OwnFormattingChannel for formatting.
  *
  * Uses the systemd journal native protocol via /run/systemd/journal/socket
  * to send structured log messages with proper priority levels, query IDs, and thread IDs.
  *
  * This handles multi-line messages (like stack traces) correctly by sending
  * them as a single journal entry, unlike simple stdout/stderr which would
  * split them into separate entries.
  *
  * Call open() then isConnected() to check if the journal is available.
  *
  * Reference: https://systemd.io/JOURNAL_NATIVE_PROTOCOL/
  */
class SystemdJournalChannel : public Poco::Channel
{
public:
    SystemdJournalChannel();
    ~SystemdJournalChannel() override;

    void open() override;
    void close() override {}
    void log(const Poco::Message & msg) override;
    void logExtended(const ExtendedLogMessage & msg);

    /// Check if the journal socket was successfully connected after open()
    bool isConnected() const { return journal_fd >= 0; }

private:
    static int getSyslogPriority(Poco::Message::Priority priority);
    static void sendToJournal(
        const Poco::Message & msg,
        int priority,
        const std::string & query_id,
        uint64_t thread_id,
        uint64_t time_in_microseconds);

    static int journal_fd;
    static std::once_flag init_once_flag;
};

}
