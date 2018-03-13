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
    std::size_t num_threads;
    std::size_t num_txs;
    std::size_t num_retries;
    std::vector<std::string> tx_profile_paths;
    std::size_t tx_len_min;
    std::size_t tx_len_max;
    std::string unit = "s";
};

enum class OpCode { Get, Put, Ins, Del };

struct TransactionProfile {
    using Ptr = std::shared_ptr<TransactionProfile>;

    std::string name;
    double prob;
    // double get_prob;
    // double put_prob;
    // double ins_prob;
    // double del_prob;
    std::vector<std::pair<double, OpCode>> ops;
    std::size_t length_min;
    std::size_t length_max;
};

// using ProfileVector = std::vector<std::pair<std::size_t, TransactionProfile*>> profiles;
// enum OpCode { GET, PUT, INS, DEL };
// struct TransactionProfile {
//     std::string name;
//     std::vector<std::size_t, OpCode> ops;
//     std::normal_distribution<> length_dist;
// };
//
// TransactionProfile* select_profile(const ProfileVector& profiles, std::uniform_distribution<> uni_dist)
// {
//     // need rng engine here ...
// }

void load_profile(const std::string& path, TransactionProfile::Ptr prof)
{
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
        // {"get_prob", [&](const std::string& val){ prof->get_prob = std::stod(val); }},
        // {"put_prob", [&](const std::string& val){ prof->put_prob = std::stod(val); }},
        // {"ins_prob", [&](const std::string& val){ prof->ins_prob = std::stod(val); }},
        // {"del_prob", [&](const std::string& val){ prof->del_prob = std::stod(val); }},
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

    std::cout << "profile: " << path << std::endl;;
    std::cout << "\tname: " << prof->name << std::endl;
    std::cout << "\tprob: " << prof->prob << std::endl;
    std::cout << "\tprob_get: " << prof->ops[0].first << std::endl;
    std::cout << "\tprob_put: " << prof->ops[1].first << std::endl;
    std::cout << "\tprob_ins: " << prof->ops[2].first << std::endl;
    std::cout << "\tprob_del: " << prof->ops[3].first << std::endl;
    // std::cout << "\tprob_get: " << prof->get_prob << std::endl;
    // std::cout << "\tprob_put: " << prof->put_prob << std::endl;
    // std::cout << "\tprob_ins: " << prof->ins_prob << std::endl;
    // std::cout << "\tprob_del: " << prof->del_prob << std::endl;
    std::cout << "\tlength_min: " << prof->length_min << std::endl;
    std::cout << "\tlength_max: " << prof->length_max << std::endl;
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
    std::cout << "NAME\n";
    std::cout << "\tscaling - determine transaction throughput\n";
    std::cout << "\nSYNOPSIS\n";
    std::cout << "\tscaling options\n";
    std::cout << "\nDESCRIPTION\n";
    std::cout << "\nOPTIONS\n";
    std::cout << "\t-u, --unit UNIT\n";
    std::cout << "\t\tSets the time unit of used when printing results. Can be one of {s | ms | us | ns} (default = s).\n";
    std::cout << "\t-h, --help\n";
    std::cout << "\t\tShow this help text.\n";
}

void parse_args(int argc, char* argv[], ProgramArgs& args)
{
    static struct option longopts[] = {
        { "data"          , required_argument , NULL , 'd' },
        { "num-threads"   , required_argument , NULL , 't' },
        { "num-txs"       , required_argument , NULL , 'n' },
        { "num-retries"   , required_argument , NULL , 'r' },
//        { "mixed-profile" , required_argument , NULL , 'm' },
//        { "ronly-profile" , required_argument , NULL , 'o' },
        { "tx-profile"    , required_argument , NULL , 'p' },
        { "tx-length-min" , required_argument , NULL , 'i' },
        { "tx-length-max" , required_argument , NULL , 'a' },
        { "unit"          , required_argument , NULL , 'u' },
        { "help"          , no_argument       , NULL , 'h' },
        { NULL            , 0                 , NULL , 0 }
    };

    char ch;
    // while ((ch = getopt_long(argc, argv, "d:t:n:r:m:o:i:a:u:h", longopts, NULL)) != -1) {
    while ((ch = getopt_long(argc, argv, "d:t:n:r:p:i:a:u:h", longopts, NULL)) != -1) {
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

        // case 'm': // path to profile for mixed transactions
        //     break;

        // case 'o': // path to profile for read-only transactions
        //     break;

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

void print_args(ProgramArgs& args)
{
    std::cout << "data_path: " << args.data_path << std::endl;
    std::cout << "num_threads: " << args.num_threads << std::endl;
    std::cout << "num_txs: " << args.num_txs << std::endl;
    std::cout << "num_retries: " << args.num_retries << std::endl;
    std::cout << "profile_paths: " << std::endl;
    for (const auto& p : args.tx_profile_paths)
        std::cout << "\t" << p << std::endl;
    std::cout << "tx_len_min: " << args.tx_len_min << std::endl;
    std::cout << "tx_len_max: " << args.tx_len_max << std::endl;
    std::cout << "unit: " << args.unit << std::endl;
}

} // end namespace bench

#endif
