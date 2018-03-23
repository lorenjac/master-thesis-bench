#ifndef BENCH_UTILS_HPP
#define BENCH_UTILS_HPP

#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <getopt.h> // getopt_long
#include <unordered_map>
#include <functional>
#include <memory>

#include "workload.hpp"

namespace bench {

using KVPair = std::pair<std::string, std::string>;

struct ProgramArgs {
    std::string data_path;
    std::size_t num_threads = 1;
    std::size_t num_retries = 0;
    std::string workload_file;
    std::string unit = "s";
    bool verbose = false;
};

int read_pairs(const std::string& path, std::vector<KVPair>& pairs)
{
    // std::cout << "loading sample data from file " << path << "...\n";

    std::ifstream ifs{path};
    if (!ifs.is_open())
        return 1;

    std::string line;
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
    return 0;
}

double convert_duration(std::chrono::duration<double> dur, const std::string& unit = "")
{
    if (unit == "ms")
        return std::chrono::duration<double, std::milli>{dur}.count();

    if (unit == "us")
        return std::chrono::duration<double, std::micro>(dur).count();

    if (unit == "ns")
        return std::chrono::duration<double, std::nano>(dur).count();

    return dur.count();
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
    std::cout << "\n\t-p, --tx-profile FILE\n";
    std::cout << "\t\tPath to a file containing a transaction profile.\n";
    std::cout << "\t\tTransaction profiles control the kind of transactions that can be spawned.\n";
    std::cout << "\t\tThis parameter is required. Use -p again to provide more than one profile.\n";
    std::cout << "\t\tA profile has the format PARAMETER=VALUE for every line, order is arbitrary.\n";
    std::cout << "\t\tThe parameters are as follows:\n";
    std::cout << "\t\t  * name - name of the profile (string)\n";
    std::cout << "\t\t  * prob - probability of this transaction profile to be chosen (float)\n";
    std::cout << "\t\t  * get_prob - probability of a get operation (float)\n";
    std::cout << "\t\t  * put_prob - probability of a put operation (float)\n";
    std::cout << "\t\t  * ins_prob - probability of a ins operation (float)\n";
    std::cout << "\t\t  * del_prob - probability of a del operation (float)\n";
    std::cout << "\n\t-t, --num-threads INT\n";
    std::cout << "\t\tThe number of worker threads to spawn. (default = " << pargs.num_threads << ")\n";
    std::cout << "\n\t-r, --num-retries INT\n";
    std::cout << "\t\tThe number of times a transaction is restarted if it fails to commit. (default = " << pargs.num_retries << ")\n";
    std::cout << "\n\t-u, --unit UNIT\n";
    std::cout << "\t\tSets the time unit of used when printing results. Can be one of {s | ms | us | ns} (default = " << pargs.unit << ")\n";
    std::cout << "\n\t-v, --verbose\n";
    std::cout << "\t\tPrint additional info.\n";
    std::cout << "\n\t-h, --help\n";
    std::cout << "\t\tShow this help text.\n";
}

void parse_args(int argc, char* argv[], ProgramArgs& args)
{
    static struct option longopts[] = {
        { "data"          , required_argument , NULL , 'd' },
        { "num-threads"   , required_argument , NULL , 't' },
        { "num-retries"   , required_argument , NULL , 'r' },
        { "workload"      , required_argument , NULL , 'w' },
        { "unit"          , required_argument , NULL , 'u' },
        { "verbose"       , no_argument       , NULL , 'v' },
        { "help"          , no_argument       , NULL , 'h' },
        { NULL            , 0                 , NULL , 0 }
    };

    char ch;
    // while ((ch = getopt_long(argc, argv, "d:t:n:r:m:o:i:a:u:h", longopts, NULL)) != -1) {
    while ((ch = getopt_long(argc, argv, "d:t:r:w:u:hv", longopts, NULL)) != -1) {
        switch (ch) {
        case 'd': // path to data set
            args.data_path = optarg;
            break;

        case 't': // number of threads
            args.num_threads = std::stoll(optarg);
            break;

        case 'r': // number of times a transaction can be retried upon failure
            args.num_retries = std::stoll(optarg);
            break;

        case 'w': // add path to transaction profile
            args.workload_file = optarg;
            break;

        case 'u': // time unit
            args.unit = optarg;
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

bool validate_args(ProgramArgs& args)
{
    if (args.data_path.empty()) {
        std::cout << "error: no sample data provided (see option -d)\n";
        return false;
    }
    else if (args.workload_file.empty()) {
        std::cout << "error: no transaction profiles provided (see option -p)\n";
        return false;
    }
    else if (args.num_threads < 1) {
        std::cout << "error: spawning less than 1 threads is not allowed (see option -t)\n";
        return false;
    }
    else if (args.unit != "s" && args.unit != "ms" && args.unit != "us" && args.unit != "ns") {
        std::cout << "error: invalid time unit (see option -u)\n";
        return false;
    }
    return true;
}

void print_args(ProgramArgs& args)
{
    std::cout << "data_file: " << args.data_path << std::endl;
    std::cout << "num_threads: " << args.num_threads << std::endl;
    std::cout << "num_retries: " << args.num_retries << std::endl;
    std::cout << "workload_file: " << args.workload_file << std::endl;
    std::cout << "unit: " << args.unit << std::endl;
}

} // end namespace bench

#endif
