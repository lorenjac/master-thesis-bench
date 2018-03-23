#include "opcode.hpp"

#include <stdexcept>

namespace bench {
namespace tools {

std::ostream& operator<<(std::ostream& os, tx_opcode_t op)
{
    switch (op) {
    case tx_opcode_t::Get:
        os << "Get";
        break;

    case tx_opcode_t::Put:
        os << "Put";
        break;

    default:
        throw std::runtime_error("error: unexpected op code");
    }
    return os;
}

} // end namespace tools
} // end namespace bench
