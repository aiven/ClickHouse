#include <Dictionaries/ExecutablePoolDictionarySource.h>

#include <filesystem>

#include <boost/algorithm/string/split.hpp>

#include <Common/logger_useful.h>
#include <Common/LocalDateTime.h>
#include <Common/filesystemHelpers.h>

#include <Core/Settings.h>

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/ShellCommandSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Formats/formatBlock.h>

#include <Interpreters/Context.h>

#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
    extern const int DICTIONARY_ACCESS_DENIED;
    extern const int UNSUPPORTED_METHOD;
    extern const int SUPPORT_IS_DISABLED;
}

ExecutablePoolDictionarySource::ExecutablePoolDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    Block & sample_block_,
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
    ContextPtr context_)
    : dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block(sample_block_)
    , coordinator(std::move(coordinator_))
    , context(context_)
    , log(getLogger("ExecutablePoolDictionarySource"))
{
    /// Remove keys from sample_block for implicit_key dictionary because
    /// these columns will not be returned from source
    /// Implicit key means that the source script will return only values,
    /// and the correspondence to the requested keys is determined implicitly - by the order of rows in the result.
    if (configuration.implicit_key)
    {
        auto keys_names = dict_struct.getKeysNames();

        for (auto & key_name : keys_names)
        {
            size_t key_column_position_in_block = sample_block.getPositionByName(key_name);
            sample_block.erase(key_column_position_in_block);
        }
    }
}

ExecutablePoolDictionarySource::ExecutablePoolDictionarySource(const ExecutablePoolDictionarySource & other)
    : dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , sample_block(other.sample_block)
    , coordinator(other.coordinator)
    , context(Context::createCopy(other.context))
    , log(getLogger("ExecutablePoolDictionarySource"))
{
}

QueryPipeline ExecutablePoolDictionarySource::loadAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadAll method");
}

QueryPipeline ExecutablePoolDictionarySource::loadUpdatedAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadUpdatedAll method");
}

QueryPipeline ExecutablePoolDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    auto block = blockForIds(dict_struct, ids);
    return getStreamForBlock(block);
}

QueryPipeline ExecutablePoolDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return getStreamForBlock(block);
}

QueryPipeline ExecutablePoolDictionarySource::getStreamForBlock(const Block & block)
{
    String command = configuration.command;
    const auto & coordinator_configuration = coordinator->getConfiguration();

    if (coordinator_configuration.execute_direct)
    {
        auto global_context = context->getGlobalContext();
        auto user_scripts_path = global_context->getUserScriptsPath();
        auto script_path = user_scripts_path + '/' + command;

        if (!fileOrSymlinkPathStartsWith(script_path, user_scripts_path))
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Executable file {} must be inside user scripts folder {}",
                command,
                user_scripts_path);

        if (!FS::exists(script_path))
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Executable file {} does not exist inside user scripts folder {}",
                command,
                user_scripts_path);

        if (!FS::canExecute(script_path))
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Executable file {} is not executable inside user scripts folder {}",
                command,
                user_scripts_path);

        command = std::move(script_path);
    }

    auto header = std::make_shared<const Block>(block);
    auto source = std::make_shared<SourceFromSingleChunk>(header);
    auto shell_input_pipe = Pipe(std::move(source));

    ShellCommandSourceConfiguration command_configuration;
    command_configuration.read_fixed_number_of_rows = true;
    command_configuration.number_of_rows_to_read = block.rows();

    Pipes shell_input_pipes;
    shell_input_pipes.emplace_back(std::move(shell_input_pipe));

    auto pipe = coordinator->createPipe(
        command,
        configuration.command_arguments,
        std::move(shell_input_pipes),
        sample_block,
        context,
        command_configuration);

    if (configuration.implicit_key)
        pipe.addTransform(std::make_shared<TransformWithAdditionalColumns>(header, pipe.getSharedHeader()));

    return QueryPipeline(std::move(pipe));
}

bool ExecutablePoolDictionarySource::isModified() const
{
    return true;
}

bool ExecutablePoolDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool ExecutablePoolDictionarySource::hasUpdateField() const
{
    return false;
}

DictionarySourcePtr ExecutablePoolDictionarySource::clone() const
{
    return std::make_shared<ExecutablePoolDictionarySource>(*this);
}

std::string ExecutablePoolDictionarySource::toString() const
{
    size_t pool_size = coordinator->getConfiguration().pool_size;
    return "ExecutablePool size: " + std::to_string(pool_size) + " command: " + configuration.command;
}

void registerDictionarySourceExecutablePool(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const String & /*name*/,
                                 const DictionaryStructure & /* dict_struct */,
                                 const Poco::Util::AbstractConfiguration & /* config */,
                                 const std::string & /* config_prefix */,
                                 Block & /* sample_block */,
                                 ContextPtr /* global_context */,
                                 const std::string & /* default_database */,
                                 bool /* created_from_ddl */) -> DictionarySourcePtr
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Dictionary source of type `executable_pool` is disabled");
    };

    factory.registerSource("executable_pool", create_table_source);
}

}
