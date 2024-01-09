#include "btree.h"
#include "btree_page.h"

#include <cassert>

namespace shdb
{

BTree::BTree(const IndexMetadata & metadata_, Store & store_, std::optional<size_t> page_max_keys_size)
    : IIndex(metadata_), metadata_page(nullptr)
{
    if (!page_max_keys_size)
    {
        size_t internal_page_max_keys_size = BTreeInternalPage::calculateMaxKeysSize(metadata.fixedKeySizeInBytes());
        size_t leaf_page_max_keys_size = BTreeLeafPage::calculateMaxKeysSize(metadata.fixedKeySizeInBytes());
        page_max_keys_size = std::min(internal_page_max_keys_size, leaf_page_max_keys_size);
    }

    max_page_size = *page_max_keys_size;
    auto page_provider = createBTreePageProvider(metadata.getKeyMarshal(), metadata.fixedKeySizeInBytes(), max_page_size);
    index_table.setIndexTable(store_.createOrOpenIndexTable(metadata.getIndexName(), page_provider));

    bool initial_index_creation = index_table.getPageCount() == 0;

    if (initial_index_creation)
    {
        auto [allocated_metadata_page, metadata_page_index] = index_table.allocateMetadataPage();
        assert(metadata_page_index == MetadataPageIndex);

        auto [root_page, root_page_index] = index_table.allocateLeafPage();

        root_page.setPreviousPageIndex(InvalidPageIndex);
        root_page.setNextPageIndex(InvalidPageIndex);

        metadata_page = std::move(allocated_metadata_page);
        metadata_page.setRootPageIndex(root_page_index);
        metadata_page.setMaxPageSize(max_page_size);
        metadata_page.setKeySizeInBytes(metadata.fixedKeySizeInBytes());
        return;
    }

    metadata_page = index_table.getMetadataPage(MetadataPageIndex);

    if (metadata.fixedKeySizeInBytes() != metadata_page.getKeySizeInBytes())
        throw std::runtime_error(
            "BTree index inconsistency. Expected " + std::to_string(metadata_page.getKeySizeInBytes()) + " key size in bytes. Actual "
            + std::to_string(metadata.fixedKeySizeInBytes()));

    if (max_page_size != metadata_page.getMaxPageSize())
        throw std::runtime_error(
            "BTree index inconsistency. Expected " + std::to_string(metadata_page.getMaxPageSize()) + " max page size. Actual "
            + std::to_string(max_page_size));
}

bool BTree::try_insert(PageIndex index, const IndexKey & index_key, const RowId & row_id) {
    BTreeLeafPage page = index_table.getLeafPage(index);
    if (page.getSize() < metadata_page.getMaxPageSize()) {
        page.insert(index_key, row_id);
        return true;
    }
    return false;
}

ResponseInsert BTree::descend_insert(PageIndex node_index, const IndexKey & index_key, const RowId & row_id) {
    BTreePagePtr node = index_table.getPage(node_index);
    ResponseInsert resp;

    if (node->isLeafPage()) 
    {
        auto leaf_page = index_table.getLeafPage(node_index);

        if (leaf_page.getSize() < metadata_page.getMaxPageSize()) 
        {
            resp.skip = true;
            leaf_page.insert(index_key, row_id);
            return resp;
        }

        PageIndex prev_index = leaf_page.getPreviousPageIndex();
        PageIndex next_index = leaf_page.getNextPageIndex();

        if (prev_index != InvalidPageIndex)
        {
            Row first_key = leaf_page.getMinKey();
            RowId first_value = leaf_page.getMinValue();

            if (try_insert(prev_index, first_key, first_value)) 
            {
                resp.old_key = first_key;
                leaf_page.remove(first_key);
                leaf_page.insert(index_key, row_id);
                resp.new_key = leaf_page.getMinKey();

                return resp;
            }
        }

        if (next_index != InvalidPageIndex) 
        {
            resp.old_key = index_table.getLeafPage(next_index).getMinKey();
            Row last_key = leaf_page.getMaxKey();
            RowId last_value = leaf_page.getMaxValue();

            if (compareRows(last_key, index_key) == -1) 
            {
                if (try_insert(next_index, index_key, row_id)) 
                {
                    resp.new_key = index_key;
                    return resp;
                }
            } 
            else 
            {
                if (try_insert(next_index, last_key, last_value)) 
                {
                    resp.new_key = last_key;
                    leaf_page.remove(last_key);
                    leaf_page.insert(index_key, row_id);

                    return resp;
                }
            }
        }

        auto [new_leaf_page, new_page_index] = index_table.allocateLeafPage();

        resp.new_page = true;
        resp.page = new_page_index;

        leaf_page.setNextPageIndex(new_page_index);


        new_leaf_page.setPreviousPageIndex(node_index);
        new_leaf_page.setNextPageIndex(next_index);

        if (next_index != InvalidPageIndex) {
            index_table.getLeafPage(next_index).setPreviousPageIndex(new_page_index);
        }

        leaf_page.split(new_leaf_page);

        resp.old_key = new_leaf_page.getMinKey();
        resp.new_key = new_leaf_page.getMinKey();

        if (compareRows(index_key, new_leaf_page.getMinKey()) == -1) 
        {
            leaf_page.insert(index_key, row_id);
        }
        else
        {
            new_leaf_page.insert(index_key, row_id);
            resp.new_key = new_leaf_page.getMinKey();
        }
    }
    else if (node->isInternalPage()) 
    {
        auto internal_page = index_table.getInternalPage(node_index);
        PageIndex index = internal_page.lookup(index_key);

        resp = descend_insert(index, index_key, row_id);

        if (resp.skip) 
        {
            return resp;
        }

        if (resp.new_page)
        {
            size_t pos;

            if (internal_page.getSize() < metadata_page.getMaxPageSize())
            {
                pos = internal_page.lookupWithIndex(resp.new_key).second;
                internal_page.insertEntry(pos + 1, resp.new_key, resp.page);
                resp.skip = true;
                return resp;
            }

            auto [new_internal_page, new_page_index] = index_table.allocateInternalPage();

            Row least_key = internal_page.split(new_internal_page);
            
            if (compareRows(least_key, resp.new_key) == -1)
            {
                pos = new_internal_page.lookupWithIndex(resp.new_key).second;
                new_internal_page.insertEntry(pos + 1, resp.new_key, resp.page);
                resp.new_key = least_key;
            }
            else
            {
                pos = internal_page.lookupWithIndex(resp.new_key).second;
                internal_page.insertEntry(pos + 1, resp.new_key, resp.page);
            }

            resp.page = new_page_index;
            resp.new_key = least_key;
        }
        else 
        {
            size_t pos = internal_page.lookupWithIndex(resp.old_key).second;
            if (compareRows(internal_page.getKey(pos), resp.old_key) == 0) 
            {
                internal_page.setRow(pos, resp.new_key);
                resp.skip = true;
                return resp;
            }
        }
    }
    else 
    {
        throw std::runtime_error("Unexpected Page type");
    }

    return resp;
}

ResponseRemove BTree::descend_remove(PageIndex node_index, const IndexKey & index_key) 
{
    BTreePagePtr node = index_table.getPage(node_index);
    ResponseRemove resp;

    if (node->isLeafPage()) 
    {
        auto leaf_page = index_table.getLeafPage(node_index);
        resp.old_key = leaf_page.getMinKey();
        leaf_page.remove(index_key);

        if (leaf_page.getSize() > 0) {
            resp.new_key = leaf_page.getMinKey();
        }
        else
        {
            PageIndex prev_index = leaf_page.getPreviousPageIndex();
            PageIndex next_index = leaf_page.getNextPageIndex();

            if (prev_index != InvalidPageIndex)
            {
                auto prev_page = index_table.getLeafPage(prev_index);
                prev_page.setNextPageIndex(next_index);
            }
            if (next_index != InvalidPageIndex)
            {
                auto next_page = index_table.getLeafPage(next_index);
                next_page.setPreviousPageIndex(prev_index);
            }

            resp.remove_page = true;
        }

        return resp;
    }
    else if (node->isInternalPage()) 
    {
        // std::cout << "INTERNAL\n";
        auto internal_page = index_table.getInternalPage(node_index);
        PageIndex index = internal_page.lookup(index_key);

        resp = descend_remove(index, index_key);

        if (resp.remove_page)
        {
            size_t pos = internal_page.lookup(resp.old_key);
            Row new_key = internal_page.removeKey(pos);

            resp.remove_page = internal_page.getSize() == 0;
            resp.new_key = new_key;

            return resp;
        }
        else 
        {
            size_t pos = internal_page.lookupWithIndex(resp.old_key).second;
            if (compareRows(internal_page.getKey(pos), resp.old_key) == 0) 
            {
                internal_page.setRow(pos, resp.new_key);
                return resp;
            }
        }
    }
    else 
    {
        throw std::runtime_error("Unexpected Page type");
    }

    return resp;

}


void BTree::insert(const IndexKey & index_key, const RowId & row_id)
{
    PageIndex root_index = metadata_page.getRootPageIndex();
    BTreePagePtr root = index_table.getPage(root_index);

    ResponseInsert resp = descend_insert(root_index, index_key, row_id);

    if (resp.skip) {
        return;
    }

    if (resp.new_page) {
        auto [new_root_page, new_root_index] = index_table.allocateInternalPage();

        metadata_page.setRootPageIndex(new_root_index);

        new_root_page.insertFirstEntry(root_index);
        new_root_page.insertEntry(1, resp.new_key, resp.page);
    }
}

bool BTree::remove(const IndexKey & index_key, const RowId &)
{
    // std::cout << "\nREMOVE " << toString(index_key) << "\n\n";
    PageIndex root_index = metadata_page.getRootPageIndex();
    BTreePagePtr root = index_table.getPage(root_index);
    // dump(std::cout);

    std::vector<RowId> result;
    lookup(index_key, result);
    if (result.empty())
    {
        // std::cout << "EMPTY\n";
        return false;
    }

    // std::cout << "REMOVING\n";

    auto resp = descend_remove(root_index, index_key);

    if (resp.remove_page) {
        auto [root_page, root_page_index] = index_table.allocateLeafPage();

        root_page.setPreviousPageIndex(InvalidPageIndex);
        root_page.setNextPageIndex(InvalidPageIndex);

        metadata_page.setRootPageIndex(root_page_index);
    }
    // std::cout << "\n\n";
    // dump(std::cout);

    return true;
}

void BTree::lookup(const IndexKey & index_key, std::vector<RowId> & result)
{
    PageIndex index = metadata_page.getRootPageIndex();

    while (true) {
        BTreePagePtr page = index_table.getPage(index);

        if (page->isLeafPage()) 
        {
            auto leaf_page = index_table.getLeafPage(index);
            auto row = leaf_page.lookup(index_key);
            if (row != std::nullopt)
            {
                result.push_back(*row);
            }
            return;
        }
        else 
        {
            auto internal_page = index_table.getInternalPage(index);
            index = internal_page.lookup(index_key);
        }
    }
}

namespace
{

class BTreeEmptyIndexIterator : public IIndexIterator
{
public:
    std::optional<std::pair<IndexKey, RowId>> nextRow() override { return {}; }
};

class BTreeIndexIterator : public IIndexIterator
{
public:
    explicit BTreeIndexIterator(
        BTreeIndexTable & index_table_,
        const std::shared_ptr<Schema> & key_schema_,
        BTreeLeafPage leaf_page_,
        size_t leaf_page_offset_,
        const KeyConditions & predicates_
        )
        : index_table(index_table_)
        , key_schema(key_schema_)
        , leaf_page(leaf_page_)
        , leaf_page_offset(leaf_page_offset_)
        , predicates(predicates_){};

    std::optional<std::pair<IndexKey, RowId>> nextRow() override 
    { 
        if (leaf_page_offset < leaf_page.getSize()) 
        {
            IndexKey row = leaf_page.getKey(leaf_page_offset);
            RowId row_id = leaf_page.getValue(leaf_page_offset);
            leaf_page_offset += 1;
            if (isRowValid(row)) 
            {
                return std::pair<IndexKey, RowId>(row, row_id);
            }
            else 
            {
                return nextRow();
            }
        } 
        else {
            auto next_page = leaf_page.getNextPageIndex();
            if (next_page != InvalidPageIndex) 
            {
                leaf_page_offset = 0;
                leaf_page = index_table.getLeafPage(next_page);

                return nextRow();
            }
        }
        return std::nullopt;
    }

private:
    bool isRowValid(const Row & key)
    {
        bool valid = true;

        for (auto pred : predicates)
        {
            size_t index = 0;
            for (size_t i = 0; i < key_schema->size(); ++i) 
            {
                if (key_schema->operator[](i).name == pred.column.name)
                {
                    index = i;
                    break;
                }
            }

            switch(pred.comparator)
            {
                case IndexComparator::equal:
                    valid &= compareValue(key[index], pred.value) == 0;
                    break;
                case IndexComparator::notEqual:
                    valid &= compareValue(key[index], pred.value) != 0;
                    break;
                case IndexComparator::greater:
                    valid &= compareValue(key[index], pred.value) == 1;
                    break;
                case IndexComparator::greaterOrEqual:
                    valid &= compareValue(key[index], pred.value) >= 0;
                    break;
                case IndexComparator::less:
                valid &= compareValue(key[index], pred.value) < 0;
                    break;
                case IndexComparator::lessOrEqual:
                    valid &= compareValue(key[index], pred.value) <= 0;
                    break; 
            }
        }
        return valid;
    }

    BTreeIndexTable & index_table;
    const std::shared_ptr<Schema> key_schema;
    BTreeLeafPage leaf_page;
    size_t leaf_page_offset;
    const KeyConditions predicates;
};

}

std::unique_ptr<IIndexIterator> BTree::read()
{
    const KeyConditions predicates = {};
    return std::make_unique<BTreeIndexIterator>(index_table, metadata.getKeySchema(),lookupLeftmostLeafPage(), 0, predicates);
}

std::unique_ptr<IIndexIterator> BTree::read(const KeyConditions & predicates)
{
    return std::make_unique<BTreeIndexIterator>(index_table, metadata.getKeySchema(), lookupLeftmostLeafPage(), 0, predicates);
}

void BTree::dump(std::ostream & stream)
{
    PageIndex pages_count = index_table.getPageCount();
    for (PageIndex i = 0; i < pages_count; ++i)
    {
        auto page = index_table.getPage(i);
        auto page_type = page->getPageType();

        stream << "Page " << i << " page type " << toString(page_type) << '\n';

        switch (page_type)
        {
            case BTreePageType::invalid: {
                break;
            }
            case BTreePageType::metadata: {
                auto metadata_page = BTreeMetadataPage(page);
                metadata_page.dump(stream);
                break;
            }
            case BTreePageType::internal: {
                auto internal_page = BTreeInternalPage(page);
                internal_page.dump(stream);
                break;
            }
            case BTreePageType::leaf: {
                auto leaf_page = BTreeLeafPage(page);
                leaf_page.dump(stream);
                break;
            }
        }
    }
}

BTreeLeafPage BTree::lookupLeafPage(const IndexKey & index_key)
{
    (void)(index_key);
    throw std::runtime_error("Not implemented");
}

BTreeLeafPage BTree::lookupLeftmostLeafPage()
{
    PageIndex page_index = metadata_page.getRootPageIndex();

    while (true)
    {
        BTreePagePtr page = index_table.getPage(page_index);

        if (page->isLeafPage()) 
        {
            return index_table.getLeafPage(page_index);
        }
        else if (page->isInternalPage())
        {
            auto internal_page = index_table.getInternalPage(page_index);
            page_index = internal_page.getValue(0);
        }
        else {
            throw std::runtime_error("Unexpected page type");
        }
    }
}

BTreePtr BTree::createIndex(const IndexMetadata & index_metadata, Store & store)
{
    return std::shared_ptr<BTree>(new BTree(index_metadata, store, {}));
}

BTreePtr BTree::createIndex(const IndexMetadata & index_metadata, size_t page_max_keys_size, Store & store)
{
    return std::shared_ptr<BTree>(new BTree(index_metadata, store, page_max_keys_size));
}

void BTree::removeIndex(const std::string & name_, Store & store)
{
    store.removeTable(name_);
}

void BTree::removeIndexIfExists(const std::string & name_, Store & store)
{
    store.removeTableIfExists(name_);
}

}
