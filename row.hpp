#ifndef _DBS_ROW_HPP
#define _DBS_ROW_HPP

//     Developed for Database Systems Assignment 2
//         Anurag Ghosh - CSD - 201302179
//          Copyright Anurag Ghosh 2015.
// Distributed under the Boost Software License, Version 1.0.
//    See Copy at http://www.boost.org/LICENSE_1_0.txt

#include <iostream>
#include <sstream>

#include <string>
#include <vector>
#include <algorithm>

namespace db {
template <typename T> class row {
  private:
    std::vector<T> tuple_;
  public:
    row(const std::vector<T> &tuple) {
        tuple_ = tuple;
    }

    row(const row &r) {
        tuple_ = r.tuple_;
    }

    row& operator=(const row &r) {
        tuple_ = r.tuple_;
    }

    std::size_t size() const {
        return tuple_.size();
    }

    std::string to_string() const {
        std::stringstream ss;
        for (int i = 0; i < tuple_.size(); ++i) {
            if (i != 0) {
                ss << " ";
            }
            ss << std::to_string(tuple_[i]);
        }
        return ss.str();
    }

    T operator[](int index) const {
        if (index > tuple_.size() || index < 0) {
            throw "IndexError: Out of bounds.";
        }
        return tuple_[index];
    }

    int get_bytes() const {
        return tuple_.size()*sizeof(T);
    }
};

template<> std::string row<std::string>::to_string() const {
    std::stringstream ss;
    for (int i = 0; i < tuple_.size(); ++i) {
        if (i != 0) {
            ss << " ";
          }
          ss << tuple_[i];
    }
    return ss.str();
}

template<> int row<std::string>::get_bytes() const {
    auto size_ = 0;
    for(auto str : tuple_) {
        size_ += str.size();
    }
    return size_;
}

std::string to_str(std::vector<row<std::string>> strings) {
    std::stringstream ss;
    for (int i = 0; i < strings.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ss << strings[i].to_string() << std::endl;
    }
    return ss.str();
}

std::string to_str(std::vector<row<std::string>> *strings) {
    std::stringstream ss;
    for (int i = 0; i < strings->size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        row<std::string> r = strings->at(i);
        ss << r.to_string() << std::endl;
    }
    return ss.str();
}

}

#endif
