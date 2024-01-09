#pragma once

#include <memory>

#include "row.h"
#include "schema.h"

namespace shdb
{

class Marshal
{
public:
    explicit Marshal(std::shared_ptr<Schema> schema);

    size_t getFixedRowSpace() const;

    size_t getRowSpace(const Row & row) const;

    void serializeRow(uint8_t * data, const Row & row) const;

    Row deserializeRow(uint8_t * data) const;

private:
    size_t calculateFixedRowSpace(uint64_t nulls) const;

    uint64_t getNulls(const Row & row) const;

    std::shared_ptr<Schema> schema;
    size_t fixed_row_space;
};

}
