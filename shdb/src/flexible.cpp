#include "flexible.h"

#include <cassert>
#include <cstring>

#include "marshal.h"
#include "row.h"
#include "table.h"

namespace shdb
{

class FlexiblePage : public ITablePage
{
public:
    FlexiblePage(std::shared_ptr<Frame> frame, std::shared_ptr<Marshal> marshal) : frame(std::move(frame)), marshal(std::move(marshal)) 
    { 
        header = reinterpret_cast<size_t *>(this->frame->getData());
    }

    RowIndex getRowCount()
    {
        return header[0];
    }

    Row getRow(RowIndex index)
    {
        size_t row_amount = header[0];
        if (row_amount <= index) {
            return Row();
        }
        size_t offset = getOffset(index);
        if (offset == 0) {
            return Row();
        }

        auto * row_data = frame->getData() + offset;

        if (static_cast<bool>(row_data[0])) 
        {
            return marshal->deserializeRow(row_data + 1);
        }

        return Row();
    }

    void deleteRow(RowIndex index)
    {
        size_t row_amount = header[0];
        if (row_amount <= index) {
            return;
        }

        size_t offset = getOffset(index);
        size_t len = getLength(index);
        if (len == 0) {
            return;
        }
        size_t min_offset = findMinOffset(offset);

        size_t count = offset - min_offset;
        auto * row_data = frame->getData();

        if (count == 0) {
            header[1] -= 2 * sizeof(size_t);
            header[0] -= 1;
        } else {
            std::memmove(row_data + min_offset + len, row_data + min_offset, count);
        }

        setZeros(index);
        moveOffsets(offset, len);
        header[1] -= len;
    }

    std::pair<bool, RowIndex> insertRow(const Row & row)
    {
        size_t len = getRowSpace(row);
        if (len + 2 * sizeof(len) > PageSize - header[1] - 2 * sizeof(header[0])) {
            return {false, -1};
        }
        size_t min_offset = findMinOffset(PageSize);
        size_t offset = min_offset - len;
        size_t row_amount = header[0];
        size_t index = 0;

        
        while (index <= row_amount) {
            if (index == row_amount) {
                header[0]++;
            }
            if (header[2 + 2 * index] == 0) {
                header[2 + 2 * index] = len;
                header[3 + 2 * index] = offset;
                break;
            }
            index++;
        }
        auto * row_data = frame->getData() + offset;
        *reinterpret_cast<bool *>(row_data) = true;
        marshal->serializeRow(row_data + 1, row);

        header[1] += len + 2 * sizeof(len) ;

        return {true, index};
    }

private:
    size_t getRowSpace(const Row & row) { return 1 + marshal->getRowSpace(row); }


    size_t findMinOffset(size_t std_offset)
    {
        size_t min_offset = std_offset;
        size_t row_amount = header[0];
        for (int i = 0; i < (int)row_amount; ++i) {
            size_t offset = getOffset(i);
            if (offset != 0) {
                min_offset = std::min(offset, min_offset);
            }
        }
        return min_offset;
    }

    size_t getOffset(RowIndex index) { return header[3 + 2 * index]; }

    size_t getLength(RowIndex index) { return header[2 + 2 * index]; }

    void setZeros(RowIndex index) 
    {
        header[3 + 2 * index] = 0;
        header[2 + 2 * index] = 0;
    }

    void moveOffsets(uint64_t offset, uint64_t len) {
        uint64_t row_amount = header[0];
        for (size_t i = 0; i < row_amount; ++i) {
            auto local_offset = header[3 + 2 * i];
            if (local_offset != 0  && local_offset < offset) {
                header[3 + 2 * i] += len;
            }
        }
    }

    std::shared_ptr<Frame> frame;
    std::shared_ptr<Marshal> marshal;
    size_t * header;
};

class FlexiblePageProvider : public IPageProvider
{
public:
    explicit FlexiblePageProvider(std::shared_ptr<Marshal> marshal) : marshal(marshal) { }

    std::shared_ptr<IPage> getPage(std::shared_ptr<Frame> frame) override { return std::make_shared<FlexiblePage>(std::move(frame), marshal); }

    std::shared_ptr<Marshal> marshal;
};

std::shared_ptr<IPageProvider> createFlexiblePageProvider(std::shared_ptr<Schema> schema)
{
    auto marshal = std::make_shared<Marshal>(std::move(schema));
    return std::make_shared<FlexiblePageProvider>(std::move(marshal));
}

}
