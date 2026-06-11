#include "SystemdJournalChannel.h"
#include <cstring>
#include <cstdint>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace
{
constexpr const char * JOURNAL_SOCKET_PATH = "/run/systemd/journal/socket";
}

namespace DB
{

int SystemdJournalChannel::journal_fd = -1;
std::once_flag SystemdJournalChannel::init_once_flag;


SystemdJournalChannel::SystemdJournalChannel() = default;

SystemdJournalChannel::~SystemdJournalChannel() = default;


void SystemdJournalChannel::open()
{
    std::call_once(init_once_flag, []()
    {
        journal_fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
        if (journal_fd >= 0)
        {
            struct sockaddr_un addr;
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            strncpy(addr.sun_path, JOURNAL_SOCKET_PATH, sizeof(addr.sun_path) - 1);

            if (connect(journal_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0)
            {
                ::close(journal_fd);
                journal_fd = -1;
            }
        }
    });
}


int SystemdJournalChannel::getSyslogPriority(Poco::Message::Priority priority)
{
    /// Map Poco priorities to syslog priorities (0-7)
    switch (priority)
    {
        case Poco::Message::PRIO_FATAL:
            return 0; /// LOG_EMERG
        case Poco::Message::PRIO_CRITICAL:
            return 2; /// LOG_CRIT
        case Poco::Message::PRIO_ERROR:
            return 3; /// LOG_ERR
        case Poco::Message::PRIO_WARNING:
            return 4; /// LOG_WARNING
        case Poco::Message::PRIO_NOTICE:
            return 5; /// LOG_NOTICE
        case Poco::Message::PRIO_INFORMATION:
            return 6; /// LOG_INFO
        case Poco::Message::PRIO_DEBUG:
        case Poco::Message::PRIO_TRACE:
        case Poco::Message::PRIO_TEST:
            return 7; /// LOG_DEBUG
    }
}


void SystemdJournalChannel::sendToJournal(
    const Poco::Message & msg,
    int priority,
    const std::string & query_id,
    uint64_t thread_id,
    uint64_t time_in_microseconds)
{
    if (journal_fd < 0)
        return;

    /// Systemd journal native protocol format:
    /// Simple fields: FIELD_NAME=field_value\n
    /// Binary fields: FIELD_NAME\n + size(le64) + data
    /// We use binary format for MESSAGE to handle multi-line messages correctly

    const std::string & text = msg.getText();

    std::string buffer;
    buffer.reserve(text.size() + 512);

    /// Add the message text using binary format to handle newlines
    /// Binary format: FIELD_NAME\nSIZE(le64)DATA
    buffer += "MESSAGE\n";
    uint64_t size = text.size();
    buffer.append(reinterpret_cast<const char *>(&size), sizeof(size));
    buffer += text;
    buffer += '\n';

    /// Add syslog priority
    buffer += "PRIORITY=";
    buffer += std::to_string(priority);
    buffer += '\n';

    /// Add ClickHouse-specific fields
    if (time_in_microseconds != 0)
    {
        buffer += "CLICKHOUSE_TIMESTAMP=";
        buffer += std::to_string(time_in_microseconds);
        buffer += '\n';
    }

    if (!msg.getSource().empty())
    {
        buffer += "CLICKHOUSE_SOURCE=";
        buffer += msg.getSource();
        buffer += '\n';
    }

    if (!query_id.empty())
    {
        buffer += "CLICKHOUSE_QUERY_ID=";
        buffer += query_id;
        buffer += '\n';
    }

    if (thread_id != 0)
    {
        buffer += "TID=";
        buffer += std::to_string(thread_id);
        buffer += '\n';
    }

    const auto & source_file = msg.getSourceFile();
    if (!source_file.empty())
    {
        buffer += "CODE_FILE=";
        buffer += source_file;
        buffer += '\n';
    }

    int source_line = msg.getSourceLine();
    if (source_line > 0)
    {
        buffer += "CODE_LINE=";
        buffer += std::to_string(source_line);
        buffer += '\n';
    }

    ssize_t sent = send(journal_fd, buffer.data(), buffer.size(), MSG_NOSIGNAL);
    (void)sent; // Ignore send errors - logging should not throw
}


void SystemdJournalChannel::logExtended(const ExtendedLogMessage & msg)
{
    if (journal_fd == -1)
        open();

    int priority = getSyslogPriority(msg.base->getPriority());
    sendToJournal(*msg.base, priority, msg.query_id, msg.thread_id, msg.time_in_microseconds);
}


void SystemdJournalChannel::log(const Poco::Message & msg)
{
    logExtended(ExtendedLogMessage::getFrom(msg));
}

}
