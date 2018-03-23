#ifndef BENCH_TYPES_HPP
#define BENCH_TYPES_HPP

#include <ostream>

namespace bench {
namespace tools {

enum class tx_opcode_t
{
    Get,
    Put
};

std::ostream& operator<<(std::ostream& os, tx_opcode_t op);

} // end namespace tools
} // end namespace bench

#endif
