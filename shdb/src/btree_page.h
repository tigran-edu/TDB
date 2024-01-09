#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <optional>

#include "bufferpool.h"
#include "comparator.h"
#include "marshal.h"
#include "page.h"
#include "row.h"
#include "table.h"

namespace shdb
{

enum BTreePageType : uint32_t
{
    invalid = 0,
    metadata,
    internal,
    leaf
};

std::string toString(BTreePageType page_type);

class BTreePage : public IPage
{
public:
    explicit BTreePage(
        std::shared_ptr<Frame> frame_, std::shared_ptr<Marshal> marshal_, uint32_t key_size_in_bytes_, uint32_t max_page_size_)
        : frame(std::move(frame_)), marshal(std::move(marshal_)), key_size_in_bytes(key_size_in_bytes_), max_page_size(max_page_size_)
    {
    }

    static constexpr size_t HeaderOffset = sizeof(BTreePageType);

    const std::shared_ptr<Frame> & getFrame() const { return frame; }

    std::shared_ptr<Frame> & getFrame() { return frame; }

    const std::shared_ptr<Marshal> & getMarshal() const { return marshal; }

    BTreePageType getPageType() const { return getValue<BTreePageType>(0); }

    void setPageType(BTreePageType btree_page_type) { setValue(0, static_cast<uint32_t>(btree_page_type)); }

    bool isInvalidPage() const { return getPageType() == BTreePageType::invalid; }

    bool isLeafPage() const { return getPageType() == BTreePageType::leaf; }

    bool isInternalPage() const { return getPageType() == BTreePageType::internal; }

    bool isMetadataPage() const { return getPageType() == BTreePageType::metadata; }

    uint32_t getMaxPageSize() const { return max_page_size; }

    uint32_t getMinPageSize() const { return max_page_size / 2; }

    template <typename T>
    const T * getPtrValue(size_t index, size_t bytes_offset = 0) const
    {
        return reinterpret_cast<T *>(frame->getData() + bytes_offset) + index;
    }

    template <typename T>
    T * getPtrValue(size_t index, size_t bytes_offset = 0)
    {
        return reinterpret_cast<T *>(frame->getData() + bytes_offset) + index;
    }

    template <typename T>
    const T & getValue(size_t index, size_t bytes_offset = 0) const
    {
        return *getPtrValue<T>(index, bytes_offset);
    }

    template <typename T>
    T & getValue(size_t index, size_t bytes_offset = 0)
    {
        return *getPtrValue<T>(index, bytes_offset);
    }

    template <typename T>
    void setValue(size_t index, const T & value, size_t bytes_offset = 0)
    {
        getValue<T>(index, bytes_offset) = value;
    }

    const uint32_t key_size_in_bytes;
    const uint32_t max_page_size;

private:
    std::shared_ptr<Frame> frame;
    std::shared_ptr<Marshal> marshal;
};

using BTreePagePtr = std::shared_ptr<BTreePage>;

/** BTreeMetadataPage, first page in BTree index.
  * Contains necessary metadata information for btree index startup.
  *
  * Header format:
  * --------------------------------------------------------------------------
  * | PageType (4) | RootPageIndex (4) | KeySizeInBytes (4) | MaxPageSize(4) |
  * --------------------------------------------------------------------------
  */
class BTreeMetadataPage
{
public:
    explicit BTreeMetadataPage(BTreePagePtr page) : page(std::move(page)) { }

    static_assert(sizeof(PageIndex) == sizeof(uint32_t));

    static constexpr size_t RootPageIndexHeaderOffset = 0;

    static constexpr size_t KeySizeInBytesHeaderOffset = 1;

    static constexpr size_t MaxPageSizeHeaderOffset = 2;

    const BTreePagePtr & getRawPage() const { return page; }

    PageIndex getRootPageIndex() const { return page->getValue<PageIndex>(RootPageIndexHeaderOffset, BTreePage::HeaderOffset); }

    void setRootPageIndex(PageIndex root_page_index)
    {
        page->setValue(RootPageIndexHeaderOffset, root_page_index, BTreePage::HeaderOffset);
    }

    uint32_t getKeySizeInBytes() const { return page->getValue<uint32_t>(KeySizeInBytesHeaderOffset, BTreePage::HeaderOffset); }

    void setKeySizeInBytes(uint32_t key_size_in_bytes)
    {
        page->setValue(KeySizeInBytesHeaderOffset, key_size_in_bytes, BTreePage::HeaderOffset);
    }

    uint32_t getMaxPageSize() const { return page->getValue<uint32_t>(MaxPageSizeHeaderOffset, BTreePage::HeaderOffset); }

    void setMaxPageSize(uint32_t max_page_size) { page->setValue(MaxPageSizeHeaderOffset, max_page_size, BTreePage::HeaderOffset); }

    std::ostream & dump(std::ostream & stream, size_t offset = 0) const
    {
        std::string offset_string(offset, ' ');

        stream << offset_string << "Root page index " << getRootPageIndex() << '\n';
        stream << offset_string << "Key size in bytes " << getKeySizeInBytes() << '\n';
        stream << offset_string << "Max page size " << getMaxPageSize() << '\n';

        return stream;
    }

private:
    BTreePagePtr page;
};

/* BTree internal page.
 * Store N indexed keys and N + 1 child page indexes (PageIndex) within internal page.
 * First key is always invalid.
 *
 *  Header format (size in bytes, 4 * 2 = 8 bytes in total):
 *  -------------------------------
 * | PageType (4) | CurrentSize(4) |
 *  -------------------------------
 *
 * Internal page format (keys are stored in order):
 *
 *  -------------------------------------------------------------------------------------------------
 * | HEADER | INVALID_KEY(1) + PAGE_INDEX(1) | KEY(2) + PAGE_INDEX(2) | ... | KEY(n) + PAGE_INDEX(n) |
 *  -------------------------------------------------------------------------------------------------
 */
class BTreeInternalPage
{
public:
    explicit BTreeInternalPage(BTreePagePtr page_) : page(std::move(page_)) { }

    static_assert(sizeof(PageIndex) == sizeof(BTreePageType));

    static_assert(sizeof(PageIndex) == sizeof(uint32_t));

    static constexpr size_t CurrentSizeHeaderIndex = 0;

    static constexpr size_t HeaderOffset = BTreePage::HeaderOffset + sizeof(uint32_t) * (CurrentSizeHeaderIndex + 1);

    static constexpr size_t calculateMaxKeysSize(uint32_t key_size_in_bytes)
    {
        return (PageSize - HeaderOffset) / (key_size_in_bytes + sizeof(PageIndex));
    }

    const BTreePagePtr & getRawPage() const { return page; }

    uint32_t getSize() const { return page->getValue<uint32_t>(CurrentSizeHeaderIndex, BTreePage::HeaderOffset); }

    void setSize(uint32_t size) { page->setValue(CurrentSizeHeaderIndex, size, BTreePage::HeaderOffset); }

    void increaseSize(uint32_t amount) { page->getValue<uint32_t>(CurrentSizeHeaderIndex, BTreePage::HeaderOffset) += amount; }

    void decreaseSize(uint32_t amount) { page->getValue<uint32_t>(CurrentSizeHeaderIndex, BTreePage::HeaderOffset) -= amount; }

    Row removeKey(size_t index) 
    {
        Row row;

        for (size_t i = index + 1; i < getSize(); ++i)
        {
            if (i == 1) {
                row = getKey(i);
                setValue(0, getValue(i));
            }
            else {
                setEntry(i - 1, getKey(i), getValue(i));
            }
        }
        decreaseSize(1);

        return row;
    }

    Row getKey(size_t index) const
    {
        std::cout << "INDEX " << index << '\n';
        uint8_t * data = getEntryStartOffset(index);
        return page->getMarshal()->deserializeRow(data);
    }

    PageIndex getValue(size_t index) const
    {
        size_t offset = HeaderOffset + getEntrySize() * index + page->key_size_in_bytes;
        return page->getValue<PageIndex>(0, offset);

    }

    /// Set value for specified index
    void setValue(size_t index, const PageIndex & value)
    {
        size_t offset = HeaderOffset + getEntrySize() * index + page->key_size_in_bytes;
        page->setValue<PageIndex>(0, value, offset);
    }

    void setRow(size_t index,  const Row & row) 
    {
        uint8_t * data = getEntryStartOffset(index);
        page->getMarshal()->serializeRow(data, row);
    }

    /// Set key and value for specified index
    void setEntry(size_t index, const Row & key, const PageIndex & value)
    {
        setValue(index, value);
        setRow(index, key);
    }

    /// Insert first value for invalid key
    void insertFirstEntry(const PageIndex & value)
    {
        setValue(0, value);
        increaseSize(1);
    }

    /// Insert key and value for specified index
    bool insertEntry(size_t index, const Row & key, const PageIndex & value)
    {
        Row new_key = key;
        PageIndex new_value = value;

        while (index < getSize()) {
            Row old_key = getKey(index);
            PageIndex old_value = getValue(index);

            setEntry(index, new_key, new_value);

            new_key = old_key;
            new_value = old_value;

            ++index;
        }

        setEntry(index, new_key, new_value);

        increaseSize(1);

        return true;
    }

    /// Lookup specified key in page
    std::pair<PageIndex, size_t> lookupWithIndex(const Row & key) const
    {
        size_t l = 0;
        size_t r = getSize() - 1;

        while (l < r) {
            size_t mid = (l + r + 1) / 2;
            auto comp_val = compareRows(getKey(mid), key);
            if (comp_val <= 0) {
                l = mid;
            } else {
                r = mid - 1;
            }
        }

        return {getValue(l), l};
    }

    /// Lookup specified key in page
    PageIndex lookup(const Row & key) const { return lookupWithIndex(key).first; }

    /** Split current page and move top half of keys to rhs_page.
      * Return top key.
      */
    Row split(BTreeInternalPage & rhs_page)
    {
        size_t counter = 1;
        size_t first_index =  getSize() / 2;
        Row first_row_in_new_page = getKey(first_index);
        rhs_page.insertFirstEntry(getValue(first_index));

        for (size_t i = first_index + 1; i < getSize(); ++i) 
        {
            rhs_page.insertEntry(counter, getKey(i), getValue(i));
            counter += 1;
        }
        
        decreaseSize(counter);

        return first_row_in_new_page;
    }

    std::ostream & dump(std::ostream & stream, size_t offset = 0) const
    {
        size_t size = getSize();

        std::string offset_string(offset, ' ');
        stream << offset_string << "Size " << size << '\n';
        for (size_t i = 0; i < size; ++i)
        {
            stream << offset_string << "I " << i << " key " << (i == 0 ? "invalid" : toString(getKey(i)));
            stream << " value " << getValue(i) << '\n';
        }

        return stream;
    }

private:
    inline uint8_t * getEntryStartOffset(size_t index) const
    {
        uint8_t * key_ptr = page->getPtrValue<uint8_t>(0, HeaderOffset);
        size_t key_offset = getEntrySize() * index;
        return key_ptr + key_offset;
    }

    inline size_t getEntrySize() const { return page->key_size_in_bytes + sizeof(PageIndex); }

    BTreePagePtr page;
};

/** Store indexed key and value. Only support unique key.
  *
  *  Header format (size in byte, 4 * 4 = 16 bytes in total):
  *  ------------------------------------------------------------------------
  * | PageType (4) | PageSize(4) | PreviousPageIndex (4) | NextPageIndex (4) |
  *  ------------------------------------------------------------------------
  *
  *  Leaf page format (keys are stored in order):
  *  --------------------------------------------------------------------
  * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n) |
  *  --------------------------------------------------------------------
  */
class BTreeLeafPage
{
public:
    explicit BTreeLeafPage(BTreePagePtr page_) : page(std::move(page_)) { }

    static_assert(sizeof(PageIndex) == sizeof(BTreePageType));

    static_assert(sizeof(PageIndex) == sizeof(uint32_t));

    static constexpr size_t PageSizeHeaderIndex = 0;

    static constexpr size_t PreviousPageIdHeaderIndex = 1;

    static constexpr size_t NextPageIdHeaderIndex = 2;

    static constexpr size_t HeaderOffset = BTreePage::HeaderOffset + sizeof(uint32_t) * (NextPageIdHeaderIndex + 1);

    static constexpr size_t calculateMaxKeysSize(uint32_t key_size_in_bytes)
    {
        return (PageSize - HeaderOffset) / (sizeof(RowId) + key_size_in_bytes);
    }

    const BTreePagePtr & getRawPage() const { return page; }

    uint32_t getSize() const { return page->getValue<uint32_t>(PageSizeHeaderIndex, BTreePage::HeaderOffset); }

    void setSize(uint32_t size) { page->setValue(PageSizeHeaderIndex, size, BTreePage::HeaderOffset); }

    void increaseSize(uint32_t amount) { page->getValue<uint32_t>(PageSizeHeaderIndex, BTreePage::HeaderOffset) += amount; }

    void decreaseSize(uint32_t amount) { page->getValue<uint32_t>(PageSizeHeaderIndex, BTreePage::HeaderOffset) -= amount; }

    PageIndex getPreviousPageIndex() const { return page->getValue<PageIndex>(PreviousPageIdHeaderIndex, BTreePage::HeaderOffset); }

    void setPreviousPageIndex(PageIndex previous_page_index)
    {
        page->setValue(PreviousPageIdHeaderIndex, previous_page_index, BTreePage::HeaderOffset);
    }

    PageIndex getNextPageIndex() const { return page->getValue<PageIndex>(NextPageIdHeaderIndex, BTreePage::HeaderOffset); }

    void setNextPageIndex(PageIndex next_page_index) { page->setValue(NextPageIdHeaderIndex, next_page_index, BTreePage::HeaderOffset); }

    Row getKey(size_t index) const
    {
        uint8_t * data = getEntryStartOffset(index);
        return page->getMarshal()->deserializeRow(data);
    }

    RowId getValue(size_t index) const
    {
        size_t offset = HeaderOffset + getEntrySize() * index + page->key_size_in_bytes;
        return page->getValue<RowId>(0, offset);
    }

    RowId getMinValue() const
    {
        size_t offset = HeaderOffset + page->key_size_in_bytes;
        return page->getValue<RowId>(0, offset);
    }

    Row getMinKey() const
    {
        uint8_t * data = getEntryStartOffset(0);
        return page->getMarshal()->deserializeRow(data);
    }

    RowId getMaxValue() const
    {
        size_t offset = HeaderOffset + getEntrySize() * (getSize() - 1) + page->key_size_in_bytes;
        return page->getValue<RowId>(0, offset);
    }

    Row getMaxKey() const
    {
        uint8_t * data = getEntryStartOffset(getSize() - 1);
        return page->getMarshal()->deserializeRow(data);
    }

    void setKey(Row & key, size_t index) const 
    {
        uint8_t * data = getEntryStartOffset(index);
        page->getMarshal()->serializeRow(data, key);
    }

    void setValue(RowId & value, size_t index) const 
    {
        size_t offset = getEntrySize() * index + HeaderOffset + page->key_size_in_bytes;
        page->setValue<RowId>(0, value, offset);
    }

    /// Insert specified key and value in page
    bool insert(const Row & key, const RowId & value)
    {
        if (getSize() >= page->getMaxPageSize()) {
            return false;
        }

        size_t index = lowerBound(key);

        if (index != getSize() && compareRows(key, getKey(index)) == 0) {
            throw "Key " + toString(key) + " already exists";
        }

        Row new_key = key;
        RowId new_value = value;

        while (index < getSize()) {
            Row old_key = getKey(index);
            RowId old_value = getValue(index);

            setKey(new_key, index);
            setValue(new_value, index);

            new_key = old_key;
            new_value = old_value;

            ++index;
        }

        setKey(new_key, index);
        setValue(new_value, index);

        increaseSize(1);

        return true;
    }

    /// Lookup specified key in page
    std::optional<RowId> lookup(const Row & key) const
    {
        size_t pos = lowerBound(key);
        if (pos == getSize() || compareRows(getKey(pos), key) != 0) {
            return std::nullopt;
        }

        return getValue(pos);
    }

    /// Return index of lower bound for specified key
    size_t lowerBound(const Row & key) const
    {
        size_t l = 0;
        size_t r = getSize();

        while (l < r) {
            size_t mid = (l + r) / 2;
            auto comp_val = compareRows(getKey(mid), key);
            if (comp_val == -1) {
                l = mid + 1;
            } else {
                r = mid;
            }
        }

        return r;
    }

    /// Remove specified key from page
    bool remove(const Row & key)
    {
        size_t size = getSize();

        if (size == 0) {
            return false;
        }

        size_t pos = lowerBound(key);

        if (pos == size || compareRows(getKey(pos), key) != 0) {
            return false;
        }

        pos += 1;

        while (pos < size) {
            Row new_key = getKey(pos);
            RowId new_val =  getValue(pos);
            setKey(new_key, pos - 1);
            setValue(new_val, pos - 1);
            pos += 1;
        }

        decreaseSize(1);

        return true;
    }

    /** Split current page and move top half of keys to rhs_page.
      * Return top key.
      */
    Row split(BTreeLeafPage & rhs_page)
    {
        for (size_t i = getSize() / 2; i < getSize(); ++i) 
        {
            rhs_page.insert(getKey(i), getValue(i));
        }

        this->decreaseSize(getSize() - (getSize() / 2));

        return rhs_page.getMinKey();
    }

    Row merge(BTreeLeafPage & another_page)
    {
        for (size_t i = 0; i < getSize(); ++i)
        {
            another_page.insert(getKey(i), getValue(i));
        }

        setSize(0);

        return another_page.getMinKey();
    }

    std::ostream & dump(std::ostream & stream, size_t offset = 0) const
    {
        size_t size = getSize();

        std::string offset_string(offset, ' ');
        stream << offset_string << "Size " << size << '\n';

        auto previous_page_index = getPreviousPageIndex();
        auto next_page_index = getNextPageIndex();

        stream << "Previous page index " << (previous_page_index == InvalidPageIndex ? "invalid" : std::to_string(previous_page_index))
               << '\n';
        stream << "Next page index " << (next_page_index == InvalidPageIndex ? "invalid" : std::to_string(next_page_index)) << '\n';

        for (size_t i = 0; i < size; ++i)
        {
            stream << offset_string << "I " << i << " key " << getKey(i);
            stream << " value " << getValue(i) << '\n';
        }

        return stream;
    }

private:
    inline uint8_t * getEntryStartOffset(size_t index) const
    {
        uint8_t * key_ptr = page->getPtrValue<uint8_t>(0, HeaderOffset);
        size_t key_offset = getEntrySize() * index;
        return key_ptr + key_offset;
    }

    inline size_t getEntrySize() const { return page->key_size_in_bytes + sizeof(RowId); }

    BTreePagePtr page;
};

std::shared_ptr<IPageProvider>
createBTreePageProvider(std::shared_ptr<Marshal> marshal, uint32_t key_size_in_bytes, uint32_t max_page_size);

}
