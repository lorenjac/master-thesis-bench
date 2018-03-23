#ifndef WORKLOAD_HPP
#define WORKLOAD_HPP

#include <string>
#include <vector>

#include "opcode.hpp"

namespace bench {
namespace tools {

struct workload_cmd_t
{
    tx_opcode_t opcode;
    size_t pos;
};

using workload_tx_t = std::vector<workload_cmd_t>;
using workload_t = std::vector<workload_tx_t>;

int parseWorkload(const std::string& filePath, workload_t& workload);
int writeWorkload(const std::string& filePath, const workload_t& work);

} // end namespace tools
} // end namespace bench

#endif
