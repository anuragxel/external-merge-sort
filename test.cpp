#ifndef _TEST_CPP
#define _TEST_CPP

//     Developed for Database Systems Assignment 2
//         Anurag Ghosh - CSD - 201302179
//          Copyright Anurag Ghosh 2015.
// Distributed under the Boost Software License, Version 1.0.
//    See Copy at http://www.boost.org/LICENSE_1_0.txt

#include <iostream>
#include <fstream>

#include "table.hpp"

int main(int argc, char **argv) {
    if (argc < 6 || argc > 7) {
        std::cout << "USAGE: merge_sort input_file output_file memory [asc|desc] column {tie_column} " << std::endl;
        return -1;
    }
    int memory_size = std::stoi(argv[3]);
    bool asc_order = (argv[4] == "asc")?true:false;
    try {
        if(argc > 6) { // most vexing parse :P
            db::table table( (std::string(argv[1])), (std::string(argv[2])),
                            memory_size, asc_order, std::string(argv[5]),
                            std::string(argv[6]) );
            table.merge_sort();
        }
        else { // c++11 brace initialization
            db::table table( std::string{argv[1]}, std::string{argv[2]},
                          memory_size, asc_order, std::string{argv[5]} );
            table.merge_sort();
        }
    }
    catch(const char *c) {
        std::cout << c << std::endl;
    }
    catch(std::string& c) {
        std::cout << c << std::endl;
    }
    return 0;
}

#endif
