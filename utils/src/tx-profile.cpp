#include "tx-profile.hpp"

#include <iostream>
#include <fstream>

#include "json/json.h"

namespace bench {
namespace tools {

int parseProfile(const std::string& profile_key, const Json::Value& node,
        tx_profiles_t& profiles);

int parseTransactionProfiles(const std::string& filePath,
        tx_profiles_t& profiles)
{
    std::ifstream file(filePath, std::ifstream::binary);
    if (!file.is_open()) {
        std::cout << "error: could not open file\n";
        return 1;
    }

    Json::Value root;
    file >> root;
    for (auto key : root.getMemberNames()) {
        if (parseProfile(key, root[key], profiles)) {
            std::cout << "error: could not parse item\n";
            return 2;
        }
    }
    return 0;
}

int parseProfile(const std::string& profile_key, const Json::Value& node,
        tx_profiles_t& profiles)
{
    auto& profile = profiles.emplace_back();

    // Set name
    profile.name = profile_key;

    // Set probability of current profile
    const Json::Value& prob = node["prob"];
    if (!prob.isNull())
        profile.prob = prob.asDouble();
    else
        return 1;

    const Json::Value& ops = node["ops"];
    if (ops.isNull())
        return 1;

    // Set probabilities of individual transaction operations
    for (auto key : ops.getMemberNames()) {
        auto value = ops[key].asDouble();
        if (key == "get") {
            profile.ops.emplace_back(tx_opcode_t::Get, value);
        }
        else if (key == "put") {
            profile.ops.emplace_back(tx_opcode_t::Put, value);
        }
    }

    // Ensure that at least one type of transaction operation was found
    if (profile.ops.empty())
        return 1;

    const Json::Value& length_min = node["length_min"];
    if (!length_min.isNull())
        profile.length_min = length_min.asDouble();

    const Json::Value& length_max = node["length_max"];
    if (!length_max.isNull())
        profile.length_max = length_max.asDouble();

    return 0;
}

} // end namespace tools
} // end namespace bench

// int main(int argc, char* argv[])
// {
//     using namespace bench::tools;
//
//     if (argc < 2)
//         return 0;
//
//     const std::string filePath = argv[1];
//     std::vector<TransactionProfile::Ptr> profiles;
//     if (parse(filePath, profiles))
//         std::cout << "error: parsing failed!\n";
//
//     return 0;
// }
