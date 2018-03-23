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

namespace bench {

using KVPair = std::pair<std::string, std::string>;

struct ProgramArgs {
    std::string data_path;
    std::size_t num_threads = 1;
    std::size_t num_txs = 1;
    std::size_t num_retries = 0;
    std::vector<std::string> tx_profile_paths;
    std::size_t tx_len_min = 2;
    std::size_t tx_len_max = 256;
    std::string unit = "s";
    bool verbose = false;
};

enum class OpCode { Get, Put, Ins, Del };
std::ostream& operator<<(std::ostream& os, OpCode op)
{
    switch (op) {
    case OpCode::Get:
        os << "Get";
        break;

    case OpCode::Put:
        os << "Put";
        break;

    case OpCode::Ins:
        os << "Ins";
        break;

    case OpCode::Del:
        os << "Del";
        break;

    default:
        throw std::runtime_error("error: unexpected op code");
    }
    return os;
}

struct TransactionProfile {
    using Ptr = std::shared_ptr<TransactionProfile>;

    std::string name;
    double prob;
    std::vector<std::pair<double, OpCode>> ops;
    std::size_t length_min;
    std::size_t length_max;
};

void load_profile(const std::string& path, TransactionProfile::Ptr prof)
{
    // std::cout << "loading transaction profile from file " << path << "...\n";

    std::ifstream ifs{path};
    if (!ifs.is_open())
        throw std::invalid_argument("error: could not open profile");

    std::unordered_map<std::string, std::function<void(const std::string&)>> handlers{
        {"name", [&](const std::string& val){ prof->name = val; }},
        {"prob", [&](const std::string& val){ prof->prob = std::stod(val); }},
        {"get_prob", [&](const std::string& val){ prof->ops.emplace_back(std::stod(val), OpCode::Get); }},
        {"put_prob", [&](const std::string& val){ prof->ops.emplace_back(std::stod(val), OpCode::Put); }},
        {"ins_prob", [&](const std::string& val){ prof->ops.emplace_back(std::stod(val), OpCode::Ins); }},
        {"del_prob", [&](const std::string& val){ prof->ops.emplace_back(std::stod(val), OpCode::Del); }},
        {"length_min", [&](const std::string& val){ prof->length_min = std::stoll(val); }},
        {"length_max", [&](const std::string& val){ prof->length_max = std::stoll(val); }}
    };

    std::string line;
    while (std::getline(ifs, line)) {
        if (!line.empty()) {
            auto pos_delim = line.find('=');
            if (pos_delim != std::string::npos) {
                auto key = line.substr(0, pos_delim); // get chars before delimiter
                auto val = line.substr(pos_delim + 1); // get chars after delimiter
                auto handler = handlers.find(key);
                if (handler != handlers.end())
                    handler->second(val);
                else
                    throw std::invalid_argument("error: unknown config parameter " + key);
            }
            else {
                throw std::invalid_argument("error: missing delimiter (;) in line");
            }
        }
    }

    // std::cout << "profile: " << path << std::endl;
    // std::cout << "  * name: " << prof->name << std::endl;
    // std::cout << "  * prob: " << prof->prob << std::endl;
    // std::cout << "  * prob_get: " << prof->ops[0].first << std::endl;
    // std::cout << "  * prob_put: " << prof->ops[1].first << std::endl;
    // std::cout << "  * prob_ins: " << prof->ops[2].first << std::endl;
    // std::cout << "  * prob_del: " << prof->ops[3].first << std::endl;
    // std::cout << "  * length_min: " << prof->length_min << std::endl;
    // std::cout << "  * length_max: " << prof->length_max << std::endl;
}

std::vector<KVPair> fetch_data(const std::string& path)
{
    // std::cout << "loading sample data from file " << path << "...\n";

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
    std::cout << "\n\t-n, --num-txs INT\n";
    std::cout << "\t\tThe number of transactions each thread has to perform. (default = " << pargs.num_txs << ")\n";
    std::cout << "\n\t-i, --tx-length-min INT\n";
    std::cout << "\t\tThe minimum number of operations enclosed in a transaction. (default = " << pargs.tx_len_min << ")\n";
    std::cout << "\n\t-a, --tx-length-max INT\n";
    std::cout << "\t\tThe maximum number of operations enclosed in a transaction. (default = " << pargs.tx_len_max << ")\n";
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
        { "num-txs"       , required_argument , NULL , 'n' },
        { "num-retries"   , required_argument , NULL , 'r' },
        { "tx-profile"    , required_argument , NULL , 'p' },
        { "tx-length-min" , required_argument , NULL , 'i' },
        { "tx-length-max" , required_argument , NULL , 'a' },
        { "unit"          , required_argument , NULL , 'u' },
        { "verbose"       , no_argument       , NULL , 'v' },
        { "help"          , no_argument       , NULL , 'h' },
        { NULL            , 0                 , NULL , 0 }
    };

    char ch;
    // while ((ch = getopt_long(argc, argv, "d:t:n:r:m:o:i:a:u:h", longopts, NULL)) != -1) {
    while ((ch = getopt_long(argc, argv, "d:t:n:r:p:i:a:u:hv", longopts, NULL)) != -1) {
        switch (ch) {
        case 'd': // path to data set
            args.data_path = optarg;
            break;

        case 't': // number of threads
            args.num_threads = std::stoll(optarg);
            break;

        case 'n': // number of transactions executed by each thread
            args.num_txs = std::stoll(optarg);
            break;

        case 'r': // number of times a transaction can be retried upon failure
            args.num_retries = std::stoll(optarg);
            break;

        case 'p': // add path to transaction profile
            args.tx_profile_paths.emplace_back(optarg);
            break;

        case 'i': // minimum number of operations in a transaction
            args.tx_len_min = std::stoll(optarg);
            break;

        case 'a': // maximum number of operations in a transaction
            args.tx_len_max = std::stoll(optarg);
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
    else if (args.tx_profile_paths.empty()) {
        std::cout << "error: no transaction profiles provided (see option -p)\n";
        return false;
    }
    else if (args.num_threads < 1) {
        std::cout << "error: spawning less than 1 threads is not allowed (see option -t)\n";
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
    else if (args.unit != "s" && args.unit != "ms" && args.unit != "us" && args.unit != "ns") {
        std::cout << "error: invalid time unit (see option -u)\n";
        return false;
    }
    return true;
}

void print_args(ProgramArgs& args)
{
    std::cout << "data_path: " << args.data_path << std::endl;
    std::cout << "num_threads: " << args.num_threads << std::endl;
    std::cout << "num_txs: " << args.num_txs << std::endl;
    std::cout << "num_retries: " << args.num_retries << std::endl;
    std::cout << "profile_paths: " << std::endl;
    for (const auto& p : args.tx_profile_paths)
        std::cout << "  * " << p << std::endl;
    std::cout << "tx_len_min: " << args.tx_len_min << std::endl;
    std::cout << "tx_len_max: " << args.tx_len_max << std::endl;
    std::cout << "unit: " << args.unit << std::endl;
}

} // end namespace bench

#endif
