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
#include <queue>

#include <random>
#include <memory>
#include <ctime>

#include "row.hpp"

#define _MAX_FILE_DESCRIPTORS_ 593850

namespace db {

template <typename stream>
void reopen(stream& pStream, const std::string& pFile,
            std::ios_base::openmode pMode = std::ios_base::out)
{
    pStream.close();
    pStream.clear();
    pStream.open(pFile, pMode);
}

class bool_wrapper
{
  private:
    bool m_value_;
  public:
    bool_wrapper(): m_value_(){}
    bool_wrapper( bool value ) : m_value_(value){}
    operator bool() const { return m_value_;}
    bool* operator& () { return &m_value_; }
    const bool * const operator& () const { return &m_value_; }
    bool operator==(const bool_wrapper& b) { return m_value_ == b.m_value_; }
    bool operator!=(const bool_wrapper& b) { return m_value_ != b.m_value_; }
    bool operator==(const bool b) { return m_value_ == b; }
    bool operator!=(const bool b) { return m_value_ != b; }
};

class table {
  private:
    std::string name_; // table Name
    std::map<std::string, std::pair<int,int>> fields_; // Fields Metadata - { fieldname, (size, index) }
    int sort_by_,order_by_; // column indices
    int row_size_; // size of one row in bytes.
    std::vector<int> sizes_;
    long long int memory_size_; // size of total memory to be used.
    long long int avail_memory_; // size of available memory -  intially all available
    std::ifstream input_file_; // input file stream
    std::fstream output_file_; // output file stream
    bool asc_order_;
    typedef row<std::string> string_row;

    void get_next_field(std::ifstream &str, int index) {
        std::string line, fieldname, cell;
        std::getline(str, line);
        std::stringstream lineStream(line);
        int size = 0, iter = 0;
        while(std::getline(lineStream, cell, ',')) {
            if(iter == 0) fieldname = cell;
            if(iter == 1) {
                size = std::stoi(cell);
                sizes_.push_back(size);
            }
            if(iter > 1) throw "Error: Metadata File Invalid!";
            iter += 1;
        }
        fields_[fieldname] = std::make_pair(size, index);
    }

    template <typename file>
    string_row get_next_row(file& filestream, int seek = 0) {
        if(!filestream) throw "FileError: EOF reached!";
        if(seek > 0) {
            filestream.seekg(filestream.cur + seek);
        }
        std::string line, fieldname, cell;
        std::getline(filestream, line);
        if(line.size() == 0) return string_row(std::vector<std::string>());
        int last = 0;
        std::vector<std::string> tuple;
        for(auto& size : sizes_) {
            tuple.push_back(line.substr(last, size));
            last += size + 2; // for two spaces
        }
        return string_row(tuple);
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


    template<typename T, typename V>
    bool vector_equal(std::vector<T> vec, V value) {
        for(auto& val : vec) {
            if(val != value) return false;
        }
        return true;
    }

    int phase_one(std::function<bool(const string_row&,const string_row&)> cmp_func) {
      int iter = 0;
      std::fstream file;
      int itz = 0;
      while(input_file_) {
          long long int used = 0;
          std::shared_ptr<std::vector<string_row>> rows{new std::vector<string_row>};
          while(input_file_) {
              string_row r = get_next_row(input_file_, 0);
              if(r.size() == 0) break;
              rows->push_back(r);
              auto row_bytes = r.get_bytes();
              if(avail_memory_ - row_bytes <= 0) break;
              avail_memory_ = avail_memory_ - row_bytes;
              used += row_bytes;
          }
          std::sort(rows->begin(), rows->end(), cmp_func);
          if(iter == 0) file.open("0_partition", std::ios_base::out);
          else reopen(file, std::to_string(iter) + "_partition", std::ios_base::out);
          file << db::to_str(rows.get());
          iter += 1;
          if(iter > _MAX_FILE_DESCRIPTORS_) {
              throw "UnimplementedError: Too many partitions. Phase 3 needed.";
          } // Maximum number of Simultanous Open File Descriptors
          avail_memory_ += used;
          rows->clear();
        }
        return iter + 1;
    }

    void phase_two(int files,
           std::function<bool(const string_row&,const string_row&)> cmp_func) {

        std::vector<std::fstream> partitions; // the file streams
        std::vector<bool_wrapper> opened; // stores which files are opened
        std::mt19937 rng(std::time(nullptr));
        std::uniform_int_distribution<int> gen(0, files - 1); // uniform, unbiased
        // open all the partition files
        for(auto i = 0; i < files; i++) {
            auto file_name = std::string(std::to_string(i) + "_partition");
            partitions.push_back(std::fstream(file_name));
            opened.push_back(bool_wrapper(true));
        }
        std::shared_ptr<std::vector<string_row>> rows{new std::vector<string_row>};
        long long int used = 0;
        while(!vector_equal<bool_wrapper>(opened, false)) {
            int row_bytes;
            int index = gen(rng);
            while(!opened[index]) {
                index = gen(rng);
            }
            string_row min_r = get_next_row(partitions[index]);
            row_bytes = min_r.get_bytes();
            for(auto i = 0; i < partitions.size(); i++) {
                if(!partitions[i]) {
                    opened[i] = false;
                    continue;
                }
                auto r = get_next_row(partitions[i]);
                if(cmp_func(min_r, r)) { // it's what we want, till now
                    min_r = r;
                    index = i;
                    row_bytes = r.get_bytes();
                }
            }
            for(auto i = 0; i < partitions.size(); i++) {
                if(i == index) {
                    continue;
                }
                // not min, seek by one row = spaces + row_size + size('\n')
                partitions[i].seekg( -(row_bytes + fields_.size() - 1 + 1),
                                            std::ios_base::cur);
            }
            rows->push_back(min_r);
            // as memory is low, we write the rows into the output file
            // and we clear the rows to increase memory.
            if(avail_memory_ - row_bytes <= 0) {
                output_file_ << db::to_str(rows.get());
                rows->clear();
                avail_memory_ += used;
            }
            avail_memory_ = avail_memory_ - row_bytes;
            used += row_bytes;
        }
    }

    void cleanup(int files) {
        for(int i = 0; i < files; i++) {
            std::remove(std::string(std::to_string(i) + "_partition").c_str());
        }
    }

    void merge_sort() {
        std::function<bool(const string_row&,const string_row&)> cmp_func;
        if(asc_order_ and order_by_ >= 0) {
            cmp_func = [&](const string_row& a, const string_row& b) {
                          if(a[sort_by_] == b[sort_by_])
                              return a[order_by_] < b[order_by_];
                          return a[sort_by_] < b[sort_by_];
                        };
        }
        else if(!asc_order_ and order_by_ >= 0) {
            cmp_func = [&](const string_row& a, const string_row& b) {
                          if(a[sort_by_] == b[sort_by_])
                              return a[order_by_] > b[order_by_];
                          return a[sort_by_] > b[sort_by_];
                          };
        }
        else if(!asc_order_ and order_by_ < 0) {
            cmp_func = [&](const string_row& a, const string_row& b) {
                          return a[sort_by_] < b[sort_by_];
            };
        }
        else if(asc_order_ and order_by_ < 0){
            cmp_func = [&](const string_row& a, const string_row& b) {
                          return a[sort_by_] > b[sort_by_];
            };
        }
        auto files = phase_one(cmp_func);
        phase_two(files, cmp_func);
        cleanup(files);
    }
};
}

#endif
