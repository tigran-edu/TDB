#pragma once

#include "table.h"

namespace shdb
{

class ScanIterator
{
public:
    ScanIterator(std::shared_ptr<ITable> table, PageIndex page_index, RowIndex row_index) : table(table), current_page_index(page_index), current_row_index(row_index)
    {
    }

    RowId getRowId() const 
    {
        return {current_page_index, current_row_index};
    }

    Row getRow() 
    {  
        return table->getRow(getRowId());
    }

    Row operator*() 
    {  
        return getRow();
    }

    bool operator==(const ScanIterator & other) const
    {
        return other.current_page_index == current_page_index && other.current_row_index == current_row_index;
    }

    bool operator!=(const ScanIterator & other) const
    {
        return !(other == *this);
    }

    ScanIterator & operator++() 
    {
        if (current_row_index + 1 < table->getPage(current_page_index)->getRowCount()) {
            current_row_index += 1;
        } else {
            current_row_index = 0;
            current_page_index += 1;
        }
        return *this;
    }

    std::shared_ptr<ITable> table;
    PageIndex current_page_index;
    RowIndex current_row_index;

};


class Scan
{
public:
    explicit Scan(std::shared_ptr<ITable> table) : table(table)
    {

    }

    ScanIterator begin() const 
    {
        return ScanIterator(table, 0, 0);
    }

    ScanIterator end() const 
    {
        auto page_count = table->getPageCount();
        return ScanIterator(table, page_count, 0);
    }

    std::shared_ptr<ITable> table;
};

}
