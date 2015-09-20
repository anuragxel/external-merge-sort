#ifndef _DBS_TABLE_HPP
#define _DBS_TABLE_HPP

//     Developed for Database Systems Assignment 2
//         Anurag Ghosh - CSD - 201302179
//          Copyright Anurag Ghosh 2015.
// Distributed under the Boost Software License, Version 1.0.
//    See Copy at http://www.boost.org/LICENSE_1_0.txt

#include <iostream>
#include <fstream>
#include <sstream>

#include <string>
#include <map>
#include <algorithm>

#include "row.hpp"

#include <memory>

namespace db {

template <typename stream>
void reopen(stream& pStream, const std::string& pFile,
            std::ios_base::openmode pMode = std::ios_base::out)
{
    pStream.close();
    pStream.clear();
    pStream.open(pFile, pMode);
}

class table {
  private:
    std::string name_; // table Name
    std::map<std::string, std::pair<int,int>> fields_; // Fields Metadata - { fieldname, (size, index) }
    int sort_by_,order_by_; // column indices
    int row_size_; // size of one row in bytes.
    long long int memory_size_; // size of total memory to be used.
    long long int avail_memory_; // size of available memory -  intially all available
    std::ifstream input_file_; // input file stream
    std::fstream output_file_; // output file stream
    bool asc_order_;

    typedef row<std::string> string_row;
    typedef row<int> int_row;

    void get_next_field(std::ifstream &str, int index) {
        std::string line, fieldname, cell;
        std::getline(str, line);
        std::stringstream lineStream(line);
        int size = 0, iter = 0;
        while(std::getline(lineStream, cell, ',')) {
            if(iter == 0) fieldname = cell;
            if(iter == 1) size = std::stoi(cell);
            if(iter > 1) throw "Error: Metadata File Invalid!";
            iter += 1;
        }
        fields_[fieldname] = std::make_pair(size, index);
    }

    template <typename file>
    string_row get_next_row(file& filestream, int seek = 0) {
        if(seek > 0) {
            filestream.seekg(filestream.cur + seek);
        }
        std::string line, fieldname, cell;
        std::getline(filestream, line);
        std::stringstream lineStream(line);
        int size = 0;
        std::vector<std::string> tuple;
        while(std::getline(lineStream, cell, ' ')) {
            tuple.push_back(cell);
        }
        return string_row(tuple);
    }

    void print_debug() {

    }
  public:
    table(const std::string in_filep,
        const std::string out_filep, int memory_size, bool asc_order,
        std::string sort_by) {
          std::ifstream metadata_file;
          metadata_file.open("metadata.txt");
          input_file_.open(in_filep);
          output_file_.open(out_filep);
          memory_size_ = ((long long int)memory_size)*786432; // buffer size in bytes
          avail_memory_ = memory_size_;
          asc_order_ = asc_order;
          metadata_file.clear();
          metadata_file.seekg(0, std::ios::beg);
          int it = 0;
          while (metadata_file) {
              get_next_field(metadata_file, it);
              if (!metadata_file) {
                  break;
              }
              it += 1;
          }
          row_size_ = 0;
          for(auto& elem : fields_) {
              row_size_ += elem.second.first;
          }
          order_by_ = -1;
          if(is_field_present(sort_by)) sort_by_ = get_index_of_field(sort_by);
    }

    table(const std::string input_file,
        const std::string output_file, int memory_size, bool asc_order,
        const std::string sort_by, const std::string order_by) {

        table(input_file, output_file, memory_size, asc_order, sort_by);
        if(is_field_present(order_by)) order_by_ = get_index_of_field(order_by);
    }

    bool is_field_present(const std::string &cstr) const {
        auto it = fields_.find(cstr);
        return (it != fields_.end());
    }

    int get_index_of_field(const std::string &cstr) const {
        auto it = fields_.find(cstr);
        if(it == fields_.end()) {
            throw "Error: Field " + cstr + " not in table!";
        }
        std::pair<int, int> p = it->second;
        return p.second;
    }

    void merge_sort() {
        // Phase 1
        int iter = 0;
        std::fstream file;
        while(input_file_) {
            long long int used = 0;
            std::shared_ptr<std::vector<string_row>> rows{new std::vector<string_row>};
            while(true) {
              string_row r = get_next_row(input_file_, 0);
              if(!input_file_) {
                  break;
              }
              rows->push_back(r);
              auto row_bytes = r.get_bytes();
              if(avail_memory_ - row_bytes <= 0) {
                  break;
              }
              avail_memory_ = avail_memory_ - row_bytes;
              used += row_bytes;
            }
            if(asc_order_ and order_by_ >= 0) {
                std::sort(rows->begin(), rows->end(),
                          [&](const string_row& a, const string_row& b) {
                              if(a[sort_by_] == b[sort_by_])
                                  return a[order_by_] < b[order_by_];
                              return a[sort_by_] < b[sort_by_];
                });
            }
            else if(!asc_order_ and order_by_ >= 0) {
                std::sort(rows->begin(), rows->end(),
                          [&](const string_row& a, const string_row& b) {
                              if(a[sort_by_] == b[sort_by_])
                                  return a[order_by_] > b[order_by_];
                              return a[sort_by_] > b[sort_by_];
                });
            }
            else if(!asc_order_ and order_by_ < 0) {
                std::sort(rows->begin(), rows->end(),
                          [&](const string_row& a, const string_row& b) {
                              return a[sort_by_] > b[sort_by_];
                });
            }
            else if(asc_order_ and order_by_ < 0){
                std::sort(rows->begin(), rows->end(),
                          [&](const string_row& a, const string_row& b) {
                              return a[sort_by_] > b[sort_by_];
                });
            }
            if(iter == 0) file.open("0_partition", std::ios_base::out);
            else reopen(file, std::to_string(iter) + "_partition", std::ios_base::out);
            file << db::to_str(rows.get());
            iter += 1;
            if(iter > 593850) break; // Maximum number of Simultanous Open File Descriptors
            avail_memory_ += used;
            rows->clear();
        }
        // Phase 2
    }
};
}

#endif
