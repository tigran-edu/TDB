#pragma once

#include "aggregate_function.h"
#include "ast.h"
#include "database.h"
#include "rowset.h"

namespace shdb
{

class Interpreter
{
public:
    explicit Interpreter(std::shared_ptr<Database> db_);

    RowSet execute(const std::string & query);

private:
    RowSet executeSelect(const ASTSelectQueryPtr & select_query);
    void executeInsert(const ASTInsertQueryPtr & insert_query);
    void executeCreate(const ASTCreateQueryPtr & create_query);
    void executeDrop(const ASTDropQueryPtr & drop_query);

    std::shared_ptr<Database> db;
    AggregateFunctionFactory aggregate_function_factory;
};

}
