#include "comparator.h"

#include <cassert>
#include <cstdint>
#include <string>
#include <variant>

namespace shdb
{

int16_t Comparator::operator()(const Row & lhs, const Row & rhs) const
{
    for (size_t i = 0; i < lhs.size(); ++i) {
        int16_t value = compareValue(lhs[i], rhs[i]);
        if (value != 0) {
            return value;
        } 
    }
    return 0;
}

int16_t compareRows(const Row & lhs, const Row & rhs)
{
    return Comparator()(lhs, rhs);
}


bool operator<(const Null &lhs, const Null &rhs) {
    return false;
}

bool operator>(const Null &lhs, const Null &rhs) {
    return false;
}

bool operator>=(const Null &lhs, const Null &rhs) {
    return true;
}

bool operator<=(const Null &lhs, const Null &rhs) {
    return true;
}

int16_t compareValue(const Value & lhs, const Value & rhs)
{
    if (lhs < rhs) {
        return -1;
    }

    return lhs != rhs;
}
}
