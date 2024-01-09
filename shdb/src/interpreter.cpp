#include "interpreter.h"

#include "accessors.h"
#include "ast.h"
#include "ast_visitor.h"
#include "executor.h"
#include "expression.h"
#include "lexer.h"
#include "parser.hpp"
#include "row.h"
#include <regex>

namespace shdb
{

Interpreter::Interpreter(std::shared_ptr<Database> db_) : db(std::move(db_))
{
    registerAggregateFunctions(aggregate_function_factory);
}

RowSet Interpreter::execute(const std::string & query)
{
    Lexer lexer(query.c_str(), query.c_str() + query.size());
    ASTPtr result;
    std::string error;
    shdb::Parser parser(lexer, result, error);
    parser.parse();
    if (!result || !error.empty())
        throw std::runtime_error("Bad input: " + error);

    switch (result->type)
    {
        case ASTType::selectQuery:
        {
            ASTSelectQueryPtr select_query_ptr = std::static_pointer_cast<ASTSelectQuery>(result);
            std::shared_ptr<ITable> table;
            std::shared_ptr<Schema> schema;
            std::vector<std::string> schema_column_names;
            if (select_query_ptr->hasGroupBy())
            {
                for (auto & expr : select_query_ptr->getGroupBy()->getChildren())
                {
                    schema_column_names.push_back(expr->getName());
                }
            }
            else if (!select_query_ptr->from.empty())
            {
                for (auto & table_name : select_query_ptr->from)
                {
                    schema = db->findTableSchema(table_name);
                    for (auto & column : *schema)
                    {
                        if (std::find(schema_column_names.begin(), schema_column_names.end(), column.name) == schema_column_names.end())
                        {
                            schema_column_names.push_back(column.name);
                        }
                    }
                }
            }
            std::string new_query;
            std::regex pattern(R"(SELECT\s*\*|,\s*\*)");
            if (std::smatch sm; std::regex_search(query, sm, pattern))
            {
                new_query += sm.prefix();
                new_query += sm.str().substr(0, sm.str().size()- 1);

                for (auto column_name : schema_column_names)
                {
                    new_query += column_name + ", ";
                }
                new_query.pop_back();
                new_query.pop_back();
                
                new_query += sm.suffix();
            }
            else
            {
                new_query = query;
            }

            lexer = Lexer(new_query.c_str(), new_query.c_str() + new_query.size());

            auto parser2 = shdb::Parser(lexer, result, error);
            parser2.parse();

            if (!result || !error.empty())
                throw std::runtime_error("Bad input: " + error);
            return executeSelect(std::static_pointer_cast<ASTSelectQuery>(result));
        }
        case ASTType::insertQuery:
            executeInsert(std::static_pointer_cast<ASTInsertQuery>(result));
            break;
        case ASTType::createQuery:
            executeCreate(std::static_pointer_cast<ASTCreateQuery>(result));
            break;
        case ASTType::dropQuery:
            executeDrop(std::static_pointer_cast<ASTDropQuery>(result));
            break;
        default:
            throw std::runtime_error("Invalid AST. Expected SELECT, INSERT, CREATE or DROP query");
    }

    return RowSet{};
}

RowSet Interpreter::executeSelect(const ASTSelectQueryPtr & select_query_ptr)
{
    if (select_query_ptr->from.empty())
    {
        auto read_from_rows_exec = createReadFromRowsExecutor({}, nullptr);
        auto children = select_query_ptr->getProjection()->getChildren();
        auto expressions = buildExpressions(children, nullptr);
        auto expr_exec  = createExpressionsExecutor(std::move(read_from_rows_exec), expressions);
        auto row_set = RowSet(expr_exec->getOutputSchema(), {*expr_exec->next()});
        return row_set;
    }
    else
    {
        ExecutorPtr executor = nullptr;
        for (auto table_name : select_query_ptr->from)
        {
            auto table = db->getTable(table_name);
            auto schema = db->findTableSchema(table_name);
            if (executor != nullptr)
            {
                auto tmp_executor = createReadFromTableExecutor(table, schema);
                executor = createJoinExecutor(std::move(tmp_executor), std::move(executor));
            }
            else 
            {
                executor = createReadFromTableExecutor(table, schema);
            }
        }
        auto schema_accessor = std::make_shared<SchemaAccessor>(SchemaAccessor(executor->getOutputSchema()));

        if (select_query_ptr->getWhere())
        {
            auto expression = buildExpression(select_query_ptr->getWhere(), schema_accessor);
            executor = createFilterExecutor(std::move(executor), expression);
        }

        if (select_query_ptr->hasGroupBy()) {
            auto aff = AggregateFunctionFactory();
            registerAggregateFunctions(aff);

            ASTs expressions_with_aggr = collectAggregateFunctions(select_query_ptr->getProjection()->getChildren(), aff);

            GroupByExpressions expressions;
            GroupByKeys keys;
            for (auto expr_with_aggr : expressions_with_aggr)
            {
                auto expression = reinterpret_pointer_cast<ASTFunction>(expr_with_aggr);
                auto child = expression->getArguments()->getChildren();
                expressions.emplace_back(aff.getAggregateFunctionOrNull(expression->name, {Type::int64}), buildExpressions(child, schema_accessor), expression->getName());
            }

            if (select_query_ptr->getHaving())
            {
                expressions_with_aggr = collectAggregateFunctions(select_query_ptr->getHaving()->getChildren(), aff);
                for (auto expr_with_aggr : expressions_with_aggr)
                {
                    auto expression = reinterpret_pointer_cast<ASTFunction>(expr_with_aggr);
                    auto child = expression->getArguments()->getChildren();
                    bool new_ = true;
                    for (auto & ex : expressions)
                    {
                        if (ex.aggregate_function_column_name == expression->getName())
                        {
                            new_ = false;
                            break;
                        }
                    }
                    if (new_)
                    {
                         expressions.emplace_back(aff.getAggregateFunctionOrNull(expression->name, {Type::int64}), buildExpressions(child, schema_accessor), expression->getName());
                    }
                }
            }

            for (auto & expression : select_query_ptr->getGroupBy()->getChildren()) {
                keys.emplace_back(buildExpression(expression, schema_accessor), expression->getName());        
            }

            executor = createGroupByExecutor(std::move(executor), keys, expressions);
            schema_accessor = std::make_shared<SchemaAccessor>(SchemaAccessor(executor->getOutputSchema()));
        }
    
        if (select_query_ptr->getHaving())
        {
            auto expression = buildExpression(select_query_ptr->getHaving(), schema_accessor);
            executor = createFilterExecutor(std::move(executor), expression);
        }

        if (select_query_ptr->getOrder())
        {
            SortExpressions expressions;
            for (auto & expression : select_query_ptr->getOrder()->getChildren())
            {
                auto expr = std::reinterpret_pointer_cast<ASTOrder>(expression);
                if (schema_accessor->hasColumn(expr->getName()))
                {
                    std::string name = expr->getName();
                    auto new_col = std::make_shared<ASTIdentifier>(ASTIdentifier(name));
                    expressions.push_back({buildExpression(new_col, schema_accessor), expr->desc});
                }
                else 
                {
                    expressions.push_back({buildExpression(expr->getExpr(), schema_accessor), expr->desc});
                }
            }
            executor = createSortExecutor(std::move(executor), expressions);
        }

        auto children = select_query_ptr->getProjection()->getChildren();
        auto expressions = buildExpressions(children, schema_accessor);
        executor  = createExpressionsExecutor(std::move(executor), expressions);
    
        return shdb::execute(std::move(executor));
    }
}

void Interpreter::executeInsert(const std::shared_ptr<ASTInsertQuery> & insert_query)
{
    auto schema = db->findTableSchema(insert_query->table);
    auto table = db->getTable(insert_query->table, schema);

    auto read_from_rows_exec = createReadFromRowsExecutor({}, nullptr);
    auto children = insert_query->getValues()->getChildren();
    auto expressions = buildExpressions(children, nullptr);
    auto expr_exec  = createExpressionsExecutor(std::move(read_from_rows_exec), expressions);
    auto row_set = RowSet(expr_exec->getOutputSchema(), {*expr_exec->next()});

    auto row_schema = row_set.getSchema();
    if (row_schema->size() != schema->size()) 
    {
        throw std::runtime_error("Wrong schema");
    }
    for (size_t i = 0; i < schema->size(); ++i)
    {
        if ((*row_schema)[i].type != (*schema)[i].type) {
            throw std::runtime_error("Wrong schema");
        }
    }

    table->insertRow(row_set.getRows()[0]);
}

void Interpreter::executeCreate(const ASTCreateQueryPtr & create_query)
{
    db->createTable(create_query->table, create_query->schema);
}

void Interpreter::executeDrop(const ASTDropQueryPtr & drop_query)
{
    db->dropTable(drop_query->table);
}

}
