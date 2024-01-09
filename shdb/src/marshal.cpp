#include "marshal.h"

#include <cassert>
#include <cstring>
#include <iostream>

namespace shdb
{

namespace
{

template <class T>
void serializeValue(const T & value, uint8_t *& data)
{
    memcpy(data, &value, sizeof(value));
    data += sizeof(value);
}

template <class T>
T deserializeValue(uint8_t *& data)
{
    T result{};
    memcpy(&result, data, sizeof(result));
    data += sizeof(result);
    return result;
}

}

size_t Marshal::calculateFixedRowSpace(uint64_t nulls) const
{
    size_t result = sizeof(uint64_t);
    for (const auto & column : *schema)
    {
        auto null = nulls & 1;
        nulls >>= 1;
        if (null)
            continue;
        switch (column.type)
        {
            case Type::boolean: {
                result += sizeof(uint8_t);
                break;
            }
            case Type::uint64: {
                result += sizeof(uint64_t);
                break;
            }
            case Type::int64: {
                result += sizeof(int64_t);
                break;
            }
            case Type::varchar: {
                result += column.length;
                break;
            }
            case Type::string: {
                break;
            }
            default: {
                assert(0);
            }
        }
    }
    return result;
}

uint64_t Marshal::getNulls(const Row & row) const
{
    uint64_t nulls = 0;
    for (size_t index = 0; index < row.size(); ++index)
        if (std::holds_alternative<Null>(row[index]))
            nulls |= (1UL << index);
    return nulls;
}

Marshal::Marshal(std::shared_ptr<Schema> schema) : schema(std::move(schema)), fixed_row_space(calculateFixedRowSpace(0))
{
    assert(Marshal::schema->size() <= sizeof(uint64_t) * 8);
}

size_t Marshal::getFixedRowSpace() const
{
    return fixed_row_space;
}

size_t Marshal::getRowSpace(const Row & row) const
{
    auto nulls = getNulls(row);
    size_t result = calculateFixedRowSpace(nulls);
    for (size_t index = 0; index < schema->size(); ++index)
    {
        if (nulls & (1UL << index))
            continue;
        switch ((*schema)[index].type)
        {
            case Type::string: {
                result += 2 * sizeof(uint64_t);
                result += std::get<std::string>(row[index]).size();
                break;
            }
            default:
                break;
        }
    }
    return result;
}

void Marshal::serializeRow(uint8_t * data, const Row & row) const
{
    assert(row.size() < 64);
    uint64_t nulls = getNulls(row);
    auto * start = data;
    serializeValue<uint64_t>(nulls, data);
    std::vector<uint64_t> indices;
    std::vector<uint64_t> offset_poss;
    for (size_t index = 0; index < schema->size(); ++index)
    {
        if (nulls & (1UL << index))
            continue;

        switch ((*schema)[index].type)
        {
            case Type::boolean: {
                auto value = static_cast<uint8_t>(std::get<bool>(row[index]));
                serializeValue(value, data);
                break;
            }
            case Type::uint64: {
                auto value = std::get<uint64_t>(row[index]);
                serializeValue(value, data);
                break;
            }
            case Type::int64: {
                auto value = std::get<int64_t>(row[index]);
                serializeValue(value, data);
                break;
            }
            case Type::varchar: {
                const auto & str = std::get<std::string>(row[index]);
                auto length = (*schema)[index].length;
                ::memcpy(data, str.c_str(), str.size());
                ::memset(data + str.size(), 0, length - str.size());
                data += length;
                break;
            }
            case Type::string: {
                const auto & str = std::get<std::string>(row[index]);
                uint64_t len = str.size();
                uint64_t offset = 0;
                serializeValue(len, data);
                offset_poss.push_back(static_cast<size_t>(data - start));
                serializeValue(offset, data);
                indices.push_back(index);
                break;
            }
        }
    }
    for (size_t i = 0; i < indices.size(); ++i) 
    {
        auto index = indices[i];
        auto offset_pos = offset_poss[i];
        size_t offset = static_cast<size_t>(data - start);
        const auto & str = std::get<std::string>(row[index]);
        auto length = str.size();
        ::memcpy(data, str.c_str(), length);
        data += length;
        ::memcpy(start + offset_pos, &offset, sizeof(offset));
    }
    assert(static_cast<size_t>(data - start) == getRowSpace(row));
}

Row Marshal::deserializeRow(uint8_t * data) const
{
    auto * start = data;
    auto nulls = deserializeValue<uint64_t>(data);
    Row row;
    uint64_t str_len = 0;
    for (size_t index = 0; index < schema->size(); ++index)
    {
        if (nulls & (1UL << index))
        {
            row.emplace_back(Null{});
            continue;
        }
        switch ((*schema)[index].type)
        {
            case Type::boolean: {
                auto value = deserializeValue<uint8_t>(data);
                row.emplace_back(static_cast<bool>(value));
                break;
            }
            case Type::uint64: {
                auto value = deserializeValue<uint64_t>(data);
                row.emplace_back(value);
                break;
            }
            case Type::int64: {
                auto value = deserializeValue<int64_t>(data);
                row.emplace_back(value);
                break;
            }
            case Type::varchar: {
                auto length = strnlen(reinterpret_cast<char *>(data), (*schema)[index].length);
                auto str = std::string(reinterpret_cast<char *>(data), length);
                row.emplace_back(std::move(str));
                data += (*schema)[index].length;
                break;
            }
            case Type::string: {
                auto length =  deserializeValue<uint64_t>(data);
                auto offset =  deserializeValue<uint64_t>(data);
                auto str = std::string(reinterpret_cast<char *>(start + offset), length);
                row.emplace_back(std::move(str));
                str_len += length;
                break;
            }
        }
    }
    data += str_len;
    assert(static_cast<size_t>(data - start) == getRowSpace(row));

    return row;
}

}
