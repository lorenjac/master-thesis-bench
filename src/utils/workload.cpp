#include "workload.hpp"

#include <iostream>
#include <fstream>

#include "json/json.h"

namespace bench {
namespace tools {

int parseWorkloadTransaction(const size_t index, const Json::Value& node, workload_t& workload);

int readJSONFile(const std::string& filePath, Json::Value& root)
{
    std::ifstream file(filePath, std::ifstream::binary);
    if (!file.is_open()) {
        std::cout << "error: could not open file\n";
        return 1;
    }
    file >> root;
    return 0;
}

int writeJSONFile(const std::string& filePath, const Json::Value& root)
{
    std::ofstream file(filePath, std::ofstream::binary);
    if (!file.is_open()) {
        std::cout << "error: could not open file\n";
        return 1;
    }
    file << root;
    return 0;
}

int parseWorkload(const std::string& filePath, workload_t& workload)
{
    Json::Value root;
    if (readJSONFile(filePath, root))
        return 1;

    const auto work_size = root["size"].asInt();
    workload.reserve(work_size);
    workload.resize(work_size);

    const auto& txs = root["txs"];
    for (unsigned i=0; i<txs.size(); ++i) {
        if (parseWorkloadTransaction(i, txs[i], workload))
            return 1;
    }
    return 0;
}

int parseWorkloadTransaction(const size_t index, const Json::Value& tx_node, workload_t& workload)
{
    // Get i-th preallocated transaction
    auto& tx = workload[index];

    // Preallocate commands for this transactions
    const auto tx_size = tx_node["size"].asUInt64();
    tx.reserve(tx_size);
    tx.resize(tx_size);

    // Init preallocated commands for this transaction
    const auto& tx_cmds = tx_node["cmds"];
    for (unsigned i=0; i<tx_cmds.size(); ++i) {
        const auto& cmd_node = tx_cmds[i];

        // Get target position in sample data set
        const auto pos = cmd_node["pos"].asInt();
        tx[i].pos = pos;

        // Get operation to perform
        const auto cmd = cmd_node["cmd"].asString();
        if (cmd == "get") {
            tx[i].opcode = tx_opcode_t::Get;
        }
        else if (cmd == "put") {
            tx[i].opcode = tx_opcode_t::Put;
        }
        // std::cout << "cmd: " << cmd << ", pos: " << pos << std::endl;
    }
    // std::printf("\n");
    return 0;
}

int writeWorkload(const std::string& filePath, const workload_t& work)
{
    Json::Value root;
    root["size"] = static_cast<Json::UInt64>(work.size());
    auto& txs_node = root["txs"];
    for (const auto& tx : work) {
        Json::Value tx_node;
        tx_node["size"] = static_cast<Json::UInt64>(tx.size());
        auto& cmds_node = tx_node["cmds"];
        for (const auto& [cmd, pos] : tx) {
            Json::Value cmd_node;
            if (cmd == tx_opcode_t::Get) {
                cmd_node["cmd"] = "get";
            }
            else if (cmd == tx_opcode_t::Put) {
                cmd_node["cmd"] = "put";
            }
            cmd_node["pos"] = static_cast<Json::UInt64>(pos);
            cmds_node.append(cmd_node);
        }
        txs_node.append(tx_node);
    }
    return writeJSONFile(filePath, root);
}

} // end namespace tools
} // end namespace bench
