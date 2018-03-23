#include <iostream>
#include <fstream>
#include <cstddef>
#include <string>
#include <random>

namespace app
{
    constexpr char alpha[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    constexpr std::size_t alpha_size = sizeof(alpha) - 1; // do not count terminal byte
    constexpr char delim = ';';

    void usage()
    {
        std::cout << "usage: gen_data KEY_SIZE VAL_SIZE NUM_PAIRS FILE\n";
    }

    void generate_pairs(const std::size_t key_size, const std::size_t val_size,
            const std::size_t num_pairs, const std::string& file)
    {
        char* key = new char[key_size + 1];
        char* val = new char[val_size + 1];
        key[key_size] = 0;
        val[val_size] = 0;

        std::random_device rand_dev;
        std::mt19937 rng(rand_dev());
        std::uniform_int_distribution<> dist(0, alpha_size - 1);

        std::ofstream ofstream(file);
        for (std::size_t i=0; i<num_pairs; ++i) {
            for (std::size_t i=0; i<key_size; ++i) {
                key[i] = alpha[dist(rng)];
            }
            for (std::size_t i=0; i<val_size; ++i) {
                val[i] = alpha[dist(rng)];
            }
            ofstream << key << delim;
            ofstream << val << '\n';
        }

        delete[] key;
        delete[] val;
    }
}

int main(int argc, char* argv[])
{
    if (argc < 4) {
        app::usage();
        return 0;
    }

    const std::size_t key_size = std::stoi(argv[1]);
    const std::size_t val_size = std::stoi(argv[2]);
    const std::size_t num_pairs = std::stoi(argv[3]);
    const std::string file = argv[4];

    app::generate_pairs(key_size, val_size, num_pairs, file);

    return 0;
}
