#include <iostream> // std::cout, std::endl
#include <iomanip>  // std::setw, std::setfill
#include <vector>   // std::vector
#include <fstream>  // std::ifstream
#include <chrono>   // std::chrono::high_resolution_clock, std::chrono::duration
#include <cmath>    // std::ceil, std::log10
#include <algorithm>// std::min_element, std::max_element
#include <stdexcept>// std::min_element, std::max_element
#include <random>   // std::random_device, std::uniform_int_distribution
#include <sstream>

#include <getopt.h> // getopt_long

#include "tx-profile.hpp"
#include "workload.hpp"
#include "opcode.hpp"

namespace bench {
namespace tools {

// ############################################################################
// TYPES
// ############################################################################

using KVPair = std::pair<std::string, std::string>;

struct ProgramArgs
{
    std::string data_path;
    std::string prof_path;
    std::string output_path;
    std::size_t num_txs = 1;
    std::size_t tx_len_min = 2;
    std::size_t tx_len_max = 64;
    bool verbose = false;
};

// ############################################################################
// FUNCTIONS
// ############################################################################

void run(ProgramArgs& args);
std::vector<KVPair> fetch_data(const std::string& path);
void parse_args(int argc, char* argv[], ProgramArgs& args);
bool validate_args(ProgramArgs& args);
void print_args(ProgramArgs& args);
void usage();

const tx_profile_t& select_profile(const tx_profiles_t& profiles, int rand)
{
    for (const auto& prof : profiles) {
        if (rand <= prof.prob) {
            return prof;
        }
        rand -= prof.prob;
    }
    throw std::runtime_error("no tx profile for given range!");
}

const tx_opcode_t select_operation(const tx_profile_t& prof, int rand)
{
    for (auto const& [opcode, prob] : prof.ops) {
        if (rand <= prob) {
            return opcode;
        }
        rand -= prob;
    }
    throw std::runtime_error("no tx opcode for given range!");
}

void run(ProgramArgs& args)
{
    // Get sample data
    const auto pairs = fetch_data(args.data_path);

    // Get transaction profiles
    tx_profiles_t profiles;
    if (parseTransactionProfiles(args.prof_path, profiles))
        return;

    // std::sort(profiles.begin(), profiles.end(),
    //         [](auto a, auto b){ return a->prob <= b->prob; });

    const auto num_txs = args.num_txs;
    const auto tx_len_min = args.tx_len_min;
    const auto tx_len_max = args.tx_len_max;
    const auto output_path = args.output_path;

    std::random_device device;
    std::mt19937 rng(device());

    // Distribution for selecting profiles, operations
    std::uniform_int_distribution<> prob_dist{1, 100};

    // Distribution for selecting pairs
    std::uniform_int_distribution<> pair_dist{0, static_cast<int>(pairs.size() - 1)};

    // Distribution for selecting #ops in a transaction
    // double tx_len_mean = (tx_len_max - tx_len_min) / 2.0;
    // std::normal_distribution<> len_dist{
    //         tx_len_mean,
    //         std::sqrt(tx_len_mean - tx_len_min)};
    std::uniform_int_distribution<unsigned long long> len_dist{tx_len_min, tx_len_max};

    workload_t workload{num_txs};
    for (std::size_t i=0; i<num_txs; ++i) {

        // select random tx profile
        const auto& prof = select_profile(profiles, prob_dist(rng));

        // select random tx length
        const auto tx_length = std::round(len_dist(rng));

        auto& tx = workload[i];
        tx.reserve(tx_length);
        tx.resize(tx_length);

        // loop for $tx_length steps
        for (std::size_t step=0; step<tx_length; ++step) {

            // select random operation
            tx[step].opcode = select_operation(prof, prob_dist(rng));

            // select random pair
            tx[step].pos = pair_dist(rng);
        }
    }
    writeWorkload(output_path, workload);
}

std::vector<KVPair> fetch_data(const std::string& path)
{
    std::ifstream ifs{path};
    if (!ifs.is_open())
        return {};

    std::string line;
    std::vector<KVPair> pairs;
    while (std::getline(ifs, line)) {
        if (!line.empty()) {
            auto pos_delim = line.find(';');
            if (pos_delim != std::string::npos) {
                pairs.emplace_back(
                    line.substr(0, pos_delim), // get chars before delimiter
                    line.substr(pos_delim + 1) // get chars after delimiter
                );
            }
            else {
                throw std::invalid_argument("error: missing delimiter (;) in line");
            }
        }
    }
    return pairs;
}

void parse_args(int argc, char* argv[], ProgramArgs& args)
{
    static struct option longopts[] = {
        { "data"          , required_argument , NULL , 'd' },
        { "num-txs"       , required_argument , NULL , 'n' },
        { "tx-profile"    , required_argument , NULL , 'p' },
        { "tx-length-min" , required_argument , NULL , 'i' },
        { "tx-length-max" , required_argument , NULL , 'a' },
        { "output"        , required_argument , NULL , 'o' },
        { "verbose"       , no_argument       , NULL , 'v' },
        { "help"          , no_argument       , NULL , 'h' },
        { NULL            , 0                 , NULL , 0 }
    };

    char ch;
    while ((ch = getopt_long(argc, argv, "d:n:p:i:a:o:hv", longopts, NULL)) != -1) {
        switch (ch) {
        case 'd': // path to data set
            args.data_path = optarg;
            break;

        case 'n': // number of transactions executed by each thread
            args.num_txs = std::stoll(optarg);
            break;

        case 'p': // add path to transaction profile
            args.prof_path = optarg;
            break;

        case 'o': // add path to output file
            args.output_path = optarg;
            break;

        case 'i': // minimum number of operations in a transaction
            args.tx_len_min = std::stoll(optarg);
            break;

        case 'a': // maximum number of operations in a transaction
            args.tx_len_max = std::stoll(optarg);
            break;

        case 'v': // verbose mode
            args.verbose = true;
            break;

        case 'h':
            usage();
            exit(0);
            break;

        default:
            usage();
            exit(0);
        }
    }
    argc -= optind;
    argv += optind;
}

void usage()
{
    ProgramArgs pargs;
    std::cout << "NAME\n";
    std::cout << "\tscaling - determine transaction throughput\n";
    std::cout << "\nSYNOPSIS\n";
    std::cout << "\tscaling options\n";
    std::cout << "\nDESCRIPTION\n";
    std::cout << "\nOPTIONS\n";
    std::cout << "\t-d, --data FILE\n";
    std::cout << "\t\tPath to a file containing sample data pairs in CSV format. This parameter is required.\n";
    std::cout << "\n\t-o, --output FILE\n";
    std::cout << "\t\tPath to file which will contain the generated workload. This parameter is required.\n";
    std::cout << "\n\t-p, --tx-profile FILE\n";
    std::cout << "\t\tPath to a file containing a transaction profile.\n";
    std::cout << "\n\t-n, --num-txs INT\n";
    std::cout << "\t\tThe number of transactions each thread has to perform. (default = " << pargs.num_txs << ")\n";
    std::cout << "\n\t-i, --tx-length-min INT\n";
    std::cout << "\t\tThe minimum number of operations enclosed in a transaction. (default = " << pargs.tx_len_min << ")\n";
    std::cout << "\n\t-a, --tx-length-max INT\n";
    std::cout << "\t\tThe maximum number of operations enclosed in a transaction. (default = " << pargs.tx_len_max << ")\n";
    std::cout << "\n\t-v, --verbose\n";
    std::cout << "\t\tPrint additional info.\n";
    std::cout << "\n\t-h, --help\n";
    std::cout << "\t\tShow this help text.\n";
}

bool validate_args(ProgramArgs& args)
{
    if (args.data_path.empty()) {
        std::cout << "error: no sample data provided (see option -d)\n";
        return false;
    }
    else if (args.prof_path.empty()) {
        std::cout << "error: no transaction profile provided (see option -p)\n";
        return false;
    }
    else if (args.output_path.empty()) {
        std::cout << "error: no output file provided (see option -o)\n";
        return false;
    }
    else if (args.num_txs < 1) {
        std::cout << "error: spwaning less than 1 transaction is not allowed (see option -n)\n";
        return false;
    }
    else if (args.tx_len_min < 1) {
        std::cout << "error: a transaction must perform at least one operation (see option -i)\n";
        return false;
    }
    else if (args.tx_len_max < 1) {
        std::cout << "error: a transaction must perform at least one operation (see option -a)\n";
        return false;
    }
    return true;
}

void print_args(ProgramArgs& args)
{
    std::cout << "data_path: " << args.data_path << std::endl;
    std::cout << "output_path: " << args.output_path << std::endl;
    std::cout << "num_txs: " << args.num_txs << std::endl;
    std::cout << "prof_path: " << args.prof_path << std::endl;
    std::cout << "tx_len_min: " << args.tx_len_min << std::endl;
    std::cout << "tx_len_max: " << args.tx_len_max << std::endl;
}

} // end namespace tools
} // end namespace bench

int main(int argc, char* argv[])
{
    using namespace bench::tools;

    if (argc < 2) {
        usage();
        exit(0);
    }

    ProgramArgs pargs;
    parse_args(argc, argv, pargs);
    if (validate_args(pargs)) {
        if (pargs.verbose)
            print_args(pargs);
        run(pargs);
    }
    else {
        usage();
    }
    return 0;
}
