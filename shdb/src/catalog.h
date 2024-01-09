#pragma once

#include "schema.h"
#include "store.h"
#include "table.h"
#include "flexible.h"
#include "scan.h"

namespace shdb
{

class Catalog
{
public:
    explicit Catalog(std::shared_ptr<Store> store) : store(store)
    {
        std::shared_ptr<Schema> table_schema(new Schema());
        table_schema->emplace_back("id", Type::uint64, 0);
        table_schema->emplace_back("name", Type::string, 0);
        table_schema->emplace_back("Type", Type::uint64, 0);
        table_schema->emplace_back("Length", Type::uint64, 0);
        page_provider = createFlexiblePageProvider(table_schema);
    }

    void saveTableSchema(const std::filesystem::path & name, std::shared_ptr<Schema> schema)
    {
        std::filesystem::path schema_path = name;
        schema_path += "_schema";
        forgetTableSchema(schema_path);

        this->store->createTable(schema_path);

        std::shared_ptr<ITable> table = store->openTable(schema_path, this->page_provider);
        for (size_t index = 0; index < schema->size(); ++index)
        {
            Row row;
            row.emplace_back(index);
            row.emplace_back((*schema)[index].name);
            row.emplace_back(static_cast<uint64_t>((*schema)[index].type));
            row.emplace_back(static_cast<uint64_t>((*schema)[index].length));
            table->insertRow(row);
        }
    }

    std::shared_ptr<Schema> findTableSchema(const std::filesystem::path & name)
    {
        std::filesystem::path schema_path = name;
        schema_path += "_schema";
        if (!this->store->checkTableExists(schema_path)) {
            return {};
        }

        std::shared_ptr<ITable> table = store->openTable(schema_path, this->page_provider);
        auto scan = Scan(table);
        auto begin = scan.begin();
        auto end = scan.end();
        std::shared_ptr<Schema> schema(new Schema);
        while (begin != end) {
            Row row = *begin;
            ColumnSchema cs;
            cs.name = std::get<std::string>(row[1]);
            cs.type = static_cast<Type>(std::get<uint64_t>(row[2]));
            cs.length = std::get<uint64_t>(row[3]);
            ++begin;
            schema->push_back(cs);
        }

        return schema;

    }

    void forgetTableSchema(const std::filesystem::path & name)
    {
        std::filesystem::path schema_path = name;
        schema_path += "_schema";
        if (store->checkTableExists(schema_path)) {
            store->removeTable(schema_path);
        }
    }

private:
    std::shared_ptr<Store> store;
    std::shared_ptr<IPageProvider> page_provider;

};

}
