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
    std::string out_filepath_; // output file stream
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
        if(!filestream) return string_row(std::vector<std::string>());
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
          out_filepath_ = out_filep;
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

    template<typename T, typename V>
    bool vector_equal(std::vector<T> vec, V value) {
        for(auto& val : vec) {
            if(val != value) return false;
        }
        return true;
    }

    void phase_two(int files, std::function<bool(const string_row&,const string_row&)> cmp_func) {
        avail_memory_ = memory_size_;
        std::vector<std::fstream> partitions; // the file streams
        std::vector<bool_wrapper> opened;
        for(auto i = 0; i < files; i++) {
            auto file_name = std::string(std::to_string(i) + "_partition");
            partitions.push_back(std::fstream(file_name));
            opened.push_back(bool_wrapper(true));
        }
        std::ofstream output_file(out_filepath_);
        long long int used = 0;
        int itx = 0;
        std::shared_ptr<std::vector<string_row>> rows{new std::vector<string_row>};
        std::vector<string_row> all_rows;
        for(int i = 0; i < partitions.size(); i++) {
            all_rows.push_back(get_next_row(partitions[i]));
        }
        while(true) {
            int idx = 0,index = 0;
            auto row_bytes = 0;
            string_row min_r;
            for(auto& value : all_rows) {
                if(value.size() == 0 or !partitions[idx]) {
                    opened[idx] = false;
                    continue;
                }
                if(min_r.empty() or cmp_func(value, min_r)) {
                    min_r = value;
                    index = idx;
                    row_bytes = min_r.get_bytes();
                }
                idx++;
            }
            if(min_r.empty()) break;
            rows->push_back(min_r);
            all_rows[index] = get_next_row(partitions[index]);
            avail_memory_ -= row_bytes;
            used += row_bytes;
            // as memory is low, we write the rows into the output file
            // and we clear the rows to increase memory.
            if(avail_memory_ - row_size_ <= 5*row_size_) {
                output_file << db::to_str(rows.get());
                rows->clear();
                avail_memory_ += used;
                used = 0;
                output_file.flush();
            }
            min_r.clear();
            itx++;
        }
        output_file << db::to_str(rows.get());
        auto itend = std::remove_if(all_rows.begin(), all_rows.end(), [](const string_row& a) {
            return a.size() == 0;
        });
        std::vector<string_row> tmp_row;
        for(auto it =  all_rows.begin(); it != itend; it++) {
            tmp_row.push_back(*it);
        }
        std::sort(tmp_row.begin(), tmp_row.end(), cmp_func);
        output_file << db::to_str(all_rows);
        rows->clear();
        avail_memory_ += used;
        output_file.flush();
        output_file.close();
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
