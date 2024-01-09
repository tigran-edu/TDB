#pragma once

#include <cstdint>
#include "btree_page.h"
#include "database.h"
#include "row.h"
#include "table.h"

namespace shdb
{

struct ResponseInsert
{
    bool new_page = false;
    bool skip = false;
    PageIndex page;
    Row old_key;
    Row new_key;
};

struct ResponseRemove
{
    bool remove_page = false;
    Row old_key;
    Row new_key;
};


class BTree;
using BTreePtr = std::shared_ptr<BTree>;

class BTreeIndexTable
{
public:
    BTreeIndexTable() = default;

    explicit BTreeIndexTable(std::shared_ptr<IIndexTable> table_) : table(std::move(table_)) { }

    void setIndexTable(std::shared_ptr<IIndexTable> index_table) { table = std::move(index_table); }

    PageIndex getPageCount() { return table->getPageCount(); }

    std::pair<BTreeMetadataPage, RowIndex> allocateMetadataPage()
    {
        PageIndex page_index = allocatePage();
        auto raw_page = getPage(page_index);
        raw_page->setPageType(BTreePageType::metadata);

        return {BTreeMetadataPage(raw_page), page_index};
    }

    BTreeMetadataPage getMetadataPage(PageIndex page_index) { return BTreeMetadataPage(getPage(page_index)); }

    std::pair<BTreeLeafPage, RowIndex> allocateLeafPage()
    {
        PageIndex page_index = allocatePage();
        auto raw_page = getPage(page_index);
        raw_page->setPageType(BTreePageType::leaf);

        return {BTreeLeafPage(raw_page), page_index};
    }

    BTreeLeafPage getLeafPage(PageIndex page_index) { return BTreeLeafPage(getPage(page_index)); }

    std::pair<BTreeInternalPage, RowIndex> allocateInternalPage()
    {
        PageIndex page_index = allocatePage();
        auto raw_page = getPage(page_index);
        raw_page->setPageType(BTreePageType::internal);

        return {BTreeInternalPage(raw_page), page_index};
    }

    BTreeInternalPage getInternalPage(PageIndex page_index) { return BTreeInternalPage(getPage(page_index)); }

    inline BTreePagePtr getPage(PageIndex page_index) { return std::static_pointer_cast<BTreePage>(table->getPage(page_index)); }

private:
    inline PageIndex allocatePage() { return table->allocatePage(); }

    std::shared_ptr<IIndexTable> table;
};

class BTree : public IIndex
{
public:
    static BTreePtr createIndex(const IndexMetadata & index_metadata, Store & store);

    static BTreePtr createIndex(const IndexMetadata & index_metadata, size_t page_max_keys_size, Store & store);

    ResponseInsert descend_insert(PageIndex node_index, const IndexKey & index_key, const RowId & row_id);

    ResponseRemove descend_remove(PageIndex node_index, const IndexKey & index_key);

    bool try_insert(PageIndex index, const IndexKey & index_key, const RowId & row_id);

    static void removeIndex(const std::string & name_, Store & store);

    static void removeIndexIfExists(const std::string & name_, Store & store);

    void insert(const IndexKey & index_key, const RowId & row_id) override;

    bool remove(const IndexKey & index_key, const RowId & row_id) override;

    void lookup(const IndexKey & index_key, std::vector<RowId> & result) override;

    std::unique_ptr<IIndexIterator> read() override;

    std::unique_ptr<IIndexIterator> read(const KeyConditions & predicates) override;

    size_t getMaxPageSize() const { return max_page_size; }

    const BTreeIndexTable & getIndexTable() const { return index_table; }

    BTreeIndexTable & getIndexTable() { return index_table; }

    void dump(std::ostream & stream);

    static constexpr PageIndex MetadataPageIndex = 0;

private:
    BTreeLeafPage lookupLeafPage(const IndexKey & index_key);

    BTreeLeafPage lookupLeftmostLeafPage();

    BTree(const IndexMetadata & metadata_, Store & store, std::optional<size_t> page_max_keys_size);

    size_t max_page_size = 0;

    BTreeIndexTable index_table;

    BTreeMetadataPage metadata_page;
};

}
