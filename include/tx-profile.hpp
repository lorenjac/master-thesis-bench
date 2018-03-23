#ifndef TX_PROFILE_HPP
#define TX_PROFILE_HPP

#include <string>
#include <vector>
#include <memory>

#include "opcode.hpp"

namespace bench {
namespace tools {

struct TransactionProfile {
    using OpProb = std::pair<tx_opcode_t, double>;

    double prob;
    std::string name;
    std::vector<OpProb> ops;
    std::size_t length_min = 0;
    std::size_t length_max = 0;
};

using tx_profile_t = TransactionProfile;
using tx_profiles_t = std::vector<tx_profile_t>;

int parseTransactionProfiles(const std::string& filePath,
        tx_profiles_t& profiles);

} // end namespace tools
} // end namespace bench

#endif
