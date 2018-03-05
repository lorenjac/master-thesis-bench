#include <iostream>
#include <fstream>
#include <cstddef>
#include <string>
#include <random>

namespace app
{
    constexpr char alpha[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    constexpr size_t al_size = sizeof(alpha) - 1; // do not count terminal byte
    constexpr char delim = ';';

    void usage()
    {
        std::cout << "usage: gen_data KEY_SIZE VAL_SIZE NUM_PAIRS FILE\n";
    }

    std::string generateString(const size_t size)
    {
        if (!size)
            return "";

        // Configure random number generator
        std::random_device rand_dev;
        std::mt19937 rng(rand_dev());
        std::uniform_int_distribution<> dist(0, al_size - 1);

        std::string str(size, ' ');
        for (size_t i=0; i<size; ++i) {
            str[i] = alpha[dist(rng)];
        }
        return str;
    }

    void generateData(const size_t key_size, const size_t val_size,
            const size_t num_pairs, const std::string file)
    {
        std::ofstream ofstream(file);
        for (size_t i=0; i<num_pairs; ++i) {
            ofstream << generateString(key_size) << delim;
            ofstream << generateString(val_size) << '\n';
        }
    }
}

int main(int argc, char* argv[])
{
    if (argc < 4) {
        app::usage();
        return 0;
    }

    size_t key_size = std::stoi(argv[1]);
    size_t val_size = std::stoi(argv[2]);
    size_t num_pairs = std::stoi(argv[3]);
    std::string file = argv[4];
    // std::string file;
    // if (argc > 4)
    //     file = argv[4];

    app::generateData(key_size, val_size, num_pairs, file);

    return 0;
}
