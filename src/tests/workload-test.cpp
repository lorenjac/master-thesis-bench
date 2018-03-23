#include <string>
#include <iostream>

#include "workload.hpp"

int main(int argc, char* argv[])
{
    using namespace bench::tools;

    if (argc < 2)
        return 0;

    const std::string filePath = argv[1];
    workload_t work;
    if (parseWorkload(filePath, work))
        std::cout << "error: parsing failed!\n";

    std::size_t i = 0;
    std::printf("num_txs = %zu\n", work.size());
    for (const auto& tx : work) {
        std::printf("tx #%zu [size=%zu]\n", (i++), tx.size());
        for (const auto& [cmd, pos] : tx) {
            std::cout << "  " << cmd << " at " << pos << std::endl;
        }
    }
    return 0;
}
