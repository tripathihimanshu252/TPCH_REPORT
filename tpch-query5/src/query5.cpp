#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <iomanip>
#include <unordered_map>

std::mutex results_mutex;

// Helper: Splits line by '|' AND trims whitespace/hidden chars
std::vector<std::string> splitAndTrim(const std::string &line)
{
    std::vector<std::string> tokens;
    std::stringstream ss(line);
    std::string token;
    while (std::getline(ss, token, '|'))
    {
        // Remove leading and trailing whitespace/tabs/returns
        token.erase(0, token.find_first_not_of(" \t\r\n"));
        size_t last = token.find_last_not_of(" \t\r\n");
        if (last != std::string::npos)
            token.erase(last + 1);
        tokens.push_back(token);
    }
    return tokens;
}

bool parseArgs(int argc, char *argv[], std::string &r_name, std::string &start_date, std::string &end_date, int &num_threads, std::string &table_path, std::string &result_path)
{
    if (argc < 7)
        return false;
    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc) r_name = argv[++i];
        else if (arg == "--start_date" && i + 1 < argc) start_date = argv[++i];
        else if (arg == "--end_date" && i + 1 < argc) end_date = argv[++i];
        else if (arg == "--threads" && i + 1 < argc) num_threads = std::stoi(argv[++i]);
        else if (arg == "--table_path" && i + 1 < argc) table_path = argv[++i];
        else if (arg == "--result_path" && i + 1 < argc) result_path = argv[++i];
    }
    return true;
}

void loadTable(const std::string &filePath, const std::vector<std::string> &columns, std::vector<std::map<std::string, std::string>> &data)
{
    std::ifstream file(filePath);
    if (!file.is_open())
    {
        std::cerr << "Could not open file: " << filePath << std::endl;
        return;
    }
    std::string line;
    while (std::getline(file, line))
    {
        if (line.empty()) continue;
        std::vector<std::string> values = splitAndTrim(line);
        std::map<std::string, std::string> row;
        // Map based on index to ensure we get the right column even if we skip some
        for (size_t i = 0; i < columns.size() && i < values.size(); ++i)
        {
            if (columns[i] != "SKIP")
                row[columns[i]] = values[i];
        }
        data.push_back(row);
    }
}

bool readTPCHData(const std::string &path, std::vector<std::map<std::string, std::string>> &customer_data, std::vector<std::map<std::string, std::string>> &orders_data, std::vector<std::map<std::string, std::string>> &lineitem_data, std::vector<std::map<std::string, std::string>> &supplier_data, std::vector<std::map<std::string, std::string>> &nation_data, std::vector<std::map<std::string, std::string>> &region_data)
{
    // customer.tbl: custkey(0), nationkey(3)
    loadTable(path + "/customer.tbl", {"c_custkey", "SKIP", "SKIP", "c_nationkey"}, customer_data);
    
    // orders.tbl: orderkey(0), custkey(1), orderstatus(2), totalprice(3), orderdate(4)
    loadTable(path + "/orders.tbl", {"o_orderkey", "o_custkey", "SKIP", "SKIP", "o_orderdate"}, orders_data);
    
    // lineitem.tbl: orderkey(0), partkey(1), suppkey(2), linenumber(3), qty(4), extprice(5), discount(6)
    loadTable(path + "/lineitem.tbl", {"l_orderkey", "SKIP", "l_suppkey", "SKIP", "SKIP", "l_extendedprice", "l_discount"}, lineitem_data);
    
    // supplier.tbl: suppkey(0), name(1), addr(2), nationkey(3)
    loadTable(path + "/supplier.tbl", {"s_suppkey", "SKIP", "SKIP", "s_nationkey"}, supplier_data);
    
    // nation.tbl: nationkey(0), name(1), regionkey(2)
    loadTable(path + "/nation.tbl", {"n_nationkey", "n_name", "n_regionkey"}, nation_data);
    
    // region.tbl: regionkey(0), name(1)
    loadTable(path + "/region.tbl", {"r_regionkey", "r_name"}, region_data);

    std::cout << "Data Load Summary:" << std::endl;
    std::cout << " - Lineitems: " << lineitem_data.size() << std::endl;
    std::cout << " - Orders: " << orders_data.size() << std::endl;
    return true;
}

void queryWorker(int start, int end, const std::string &target_r_key,
                 const std::vector<std::map<std::string, std::string>> &lineitem_data,
                 const std::unordered_map<std::string, std::string> &order_to_cust,
                 const std::unordered_map<std::string, std::string> &cust_to_nation,
                 const std::unordered_map<std::string, std::string> &supp_to_nation,
                 const std::unordered_map<std::string, std::string> &nation_to_name,
                 const std::unordered_map<std::string, std::string> &nation_to_region,
                 std::map<std::string, double> &results)
{
    std::map<std::string, double> local_results;

    for (int i = start; i < end; ++i)
    {
        const auto &li = lineitem_data[i];
        std::string o_key = li.at("l_orderkey");

        if (order_to_cust.count(o_key))
        {
            std::string c_key = order_to_cust.at(o_key);
            std::string s_key = li.at("l_suppkey");

            if (cust_to_nation.count(c_key) && supp_to_nation.count(s_key))
            {
                std::string c_nat = cust_to_nation.at(c_key);
                std::string s_nat = supp_to_nation.at(s_key);

                // Join Condition: Customer and Supplier in same Nation and same Region
                if (c_nat == s_nat && nation_to_region.count(c_nat) && nation_to_region.at(c_nat) == target_r_key)
                {
                    double price = std::stod(li.at("l_extendedprice"));
                    double disc = std::stod(li.at("l_discount"));
                    local_results[nation_to_name.at(c_nat)] += price * (1.0 - disc);
                }
            }
        }
    }

    std::lock_guard<std::mutex> lock(results_mutex);
    for (auto const &[name, rev] : local_results)
    {
        results[name] += rev;
    }
}

bool executeQuery5(const std::string &r_name, const std::string &start_date, const std::string &end_date, int num_threads,
                   const std::vector<std::map<std::string, std::string>> &customer_data,
                   const std::vector<std::map<std::string, std::string>> &orders_data,
                   const std::vector<std::map<std::string, std::string>> &lineitem_data,
                   const std::vector<std::map<std::string, std::string>> &supplier_data,
                   const std::vector<std::map<std::string, std::string>> &nation_data,
                   const std::vector<std::map<std::string, std::string>> &region_data,
                   std::map<std::string, double> &results)
{
    std::string target_r_key = "";
    for (const auto &r : region_data)
    {
        if (r.at("r_name") == r_name)
            target_r_key = r.at("r_regionkey");
    }

    if (target_r_key == "")
    {
        std::cerr << "Region [" << r_name << "] not found!" << std::endl;
        return false;
    }

    std::unordered_map<std::string, std::string> nation_to_name, nation_to_region;
    for (const auto &n : nation_data)
    {
        nation_to_name[n.at("n_nationkey")] = n.at("n_name");
        nation_to_region[n.at("n_nationkey")] = n.at("n_regionkey");
    }

    std::unordered_map<std::string, std::string> cust_to_nation, supp_to_nation, order_to_cust;
    for (const auto &c : customer_data)
        cust_to_nation[c.at("c_custkey")] = c.at("c_nationkey");
    for (const auto &s : supplier_data)
        supp_to_nation[s.at("s_suppkey")] = s.at("s_nationkey");

    for (const auto &o : orders_data)
    {
        if (o.count("o_orderdate"))
        {
            std::string o_date = o.at("o_orderdate");
            // Compare first 10 characters to ignore hidden \r or spaces
            if (o_date.substr(0,10) >= start_date && o_date.substr(0,10) < end_date)
            {
                order_to_cust[o.at("o_orderkey")] = o.at("o_custkey");
            }
        }
    }

    std::cout << "Filtered " << order_to_cust.size() << " orders in date range." << std::endl;

    std::vector<std::thread> threads;
    int chunk_size = lineitem_data.size() / num_threads;
    if (chunk_size == 0) chunk_size = lineitem_data.size();

    for (int i = 0; i < num_threads; ++i)
    {
        int start = i * chunk_size;
        int end = (i == num_threads - 1) ? lineitem_data.size() : (i + 1) * chunk_size;
        if (start >= lineitem_data.size()) break;
        
        threads.emplace_back(queryWorker, start, end, target_r_key, std::ref(lineitem_data),
                             std::ref(order_to_cust), std::ref(cust_to_nation),
                             std::ref(supp_to_nation), std::ref(nation_to_name),
                             std::ref(nation_to_region), std::ref(results));
    }

    for (auto &t : threads) t.join();
    return true;
}

bool outputResults(const std::string &result_path, const std::map<std::string, double> &results)
{
    std::ofstream out(result_path);
    if (!out.is_open()) return false;
    for (auto const &[name, revenue] : results)
    {
        out << name << "|" << std::fixed << std::setprecision(2) << revenue << std::endl;
    }
    return true;
}