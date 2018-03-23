#include <string>
#include <vector>
#include <iostream>

#include "tx-profile.hpp"

int main(int argc, char* argv[])
{
    using namespace bench::tools;

    if (argc < 2)
        return 0;

    tx_profiles_t profiles;
    const std::string filePath = argv[1];
    if (parseTransactionProfiles(filePath, profiles))
        std::cout << "error: parsing failed!\n";

    for (const auto& p : profiles) {
        std::cout << p.name << std::endl;
        std::cout << p.prob << std::endl;
        for (auto [opc, prob] : p.ops)
            std::cout << opc << ": " << prob << std::endl;
    }
    return 0;
}
