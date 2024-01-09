#include "executor.h"
#include "comparator.h"
#include "unordered_map"

namespace shdb
{

namespace
{

class ReadFromRowsExecutor : public IExecutor
{
public:
    explicit ReadFromRowsExecutor(Rows rows_, std::shared_ptr<Schema> rows_schema_)
        : rows(std::move(rows_)), rows_schema(std::move(rows_schema_))
    {}

    std::optional<Row> next() override 
    { 
        if (counter < rows.size())
        {
            return rows[counter++];
        }
        return Row(1);
    }

    std::shared_ptr<Schema> getOutputSchema() override 
    { 
        return rows_schema;
    }

private:
    Rows rows;
    size_t counter = 0;
    std::shared_ptr<Schema> rows_schema;
};

class ReadFromTableExecutor : public IExecutor
{
public:
    explicit ReadFromTableExecutor(std::shared_ptr<ITable> table_, std::shared_ptr<Schema> table_schema_)
        : table(std::move(table_)), table_schema(std::move(table_schema_))
    {
        auto scan = Scan(table);
        iterator = std::make_shared<ScanIterator>(scan.begin());
        end = std::make_shared<ScanIterator>(scan.end());
    }

    std::optional<Row> next() override 
    {
        if (*iterator == *end)
        {
            return std::nullopt;
        }
    
        Row row = iterator->getRow();
        ++(*iterator);
        return row;
    }

    std::shared_ptr<Schema> getOutputSchema() override 
    {
        return table_schema;
    }

private:
    std::shared_ptr<ITable> table;
    std::shared_ptr<ScanIterator> iterator;
    std::shared_ptr<ScanIterator> end;
    std::shared_ptr<Schema> table_schema;
};

class ExpressionsExecutor : public IExecutor
{
public:
    explicit ExpressionsExecutor(ExecutorPtr input_executor_, Expressions expressions_)
        : input_executor(std::move(input_executor_)), expressions(std::move(expressions_))
    {}

    std::optional<Row> next() override 
    {
        auto row = input_executor->next();
        if (!row)
        {
            return std::nullopt;
        }
        Row result = Row();
        for (const auto & expression : expressions)
        {
            result.push_back(expression->evaluate(*row));
        }
        return result;
    }

    std::shared_ptr<Schema> getOutputSchema() override 
    {
        Schema output_schema = Schema();
        for (const auto & expression : expressions)
        {
            output_schema.push_back(expression->getResultType());
        }
        
        return std::make_shared<Schema>(output_schema);
    }

private:
    ExecutorPtr input_executor;
    Expressions expressions;
};

class FilterExecutor : public IExecutor
{
public:
    explicit FilterExecutor(ExecutorPtr input_executor_, ExpressionPtr filter_expression_)
        : input_executor(std::move(input_executor_)), filter_expression(std::move(filter_expression_))
    {}

    std::optional<Row> next() override 
    {
        bool value = false;
        std::optional<Row> row;
        while (!value)
        {
            row = input_executor->next();

            if (!row)
            {
                return std::nullopt;
            }
            value = std::get<bool>(filter_expression->evaluate(*row));
        }

        return row;
    }

    std::shared_ptr<Schema> getOutputSchema() override 
    {
        return input_executor->getOutputSchema();
    }

private:
    ExecutorPtr input_executor;
    ExpressionPtr filter_expression;
};

class SortExecutor : public IExecutor
{
public:
    explicit SortExecutor(ExecutorPtr input_executor_, SortExpressions sort_expressions_)
        : sort_expressions(std::move(sort_expressions_))
    {
        auto compare = [sort_expressions = sort_expressions](const Row & row1, const Row row2) -> bool
        {
            for (auto expr : sort_expressions)
            {
                bool reverse = expr.desc;
                auto val1 = expr.expression->evaluate(row1);
                auto val2 =  expr.expression->evaluate(row2);
                if (compareValue(val1, val2) == -1)
                {
                    return true ^ reverse;
                }
                if (compareValue(val1, val2) == 1)
                {
                    return false ^ reverse;
                }
            }
            return false;
        };

        RowSet row_set = shdb::execute(std::move(input_executor_));

        rows = row_set.getRows();
        std::sort(rows.begin(), rows.end(), compare);
    }

    std::optional<Row> next() override 
    {
        if (pos == rows.size())
        {
            return std::nullopt;
        }

        return rows[pos++];
    }

    std::shared_ptr<Schema> getOutputSchema() override { throw std::runtime_error("getOutputSchema cannot be called"); }

private:
    SortExpressions sort_expressions;
    Rows rows;
    size_t pos = 0;
};

class JoinExecutor : public IExecutor
{
public:
    explicit JoinExecutor(ExecutorPtr left_input_executor_, ExecutorPtr right_input_executor_)
        : left_input_executor(std::move(left_input_executor_)), right_input_executor(std::move(right_input_executor_))
    {

        auto left_output_schema = left_input_executor->getOutputSchema();
        auto right_output_schema = right_input_executor->getOutputSchema();
        schema = std::make_shared<Schema>(*left_output_schema);

        for (size_t i = 0; i < right_output_schema->size(); i++)
        {
            bool in = false;
            for (size_t j = 0; j < schema->size(); j++)
            {
                if (schema->at(j).name == right_output_schema->at(i).name)
                {
                    in = true;
                    break;
                }
            }
            if (!in)
            {
                schema->push_back(right_output_schema->at(i));
            }
        }

        if (schema->size() == left_output_schema->size() + right_output_schema->size())
        {
            return;
        }

        std::unordered_map<size_t, size_t> matching_schema_left;
        std::unordered_map<size_t, size_t> matching_schema_right;
        for (size_t i = 0; i < left_output_schema->size(); i++)
        {
            for (size_t j = 0; j < right_output_schema->size(); j++)
            {
                if (left_output_schema->at(i).name == right_output_schema->at(j).name)
                {
                    matching_schema_left[i] = j;
                    matching_schema_right[j] = i;
                    break;
                }
            }
        }
        RowSet left_rows = shdb::execute(std::move(left_input_executor));
        RowSet right_rows = shdb::execute(std::move(right_input_executor));

        for (auto left_row : left_rows.getRows())
        {
            for (auto right_row : right_rows.getRows())
            {
                bool similar = true;
                for (auto [left_pos, right_pos] : matching_schema_left)
                {
                    if (compareValue(left_row[left_pos], right_row[right_pos]) != 0)
                    {
                        similar = false;
                        break;
                    }
                }
                if (similar)
                {
                    Row new_row = left_row;
                    for (size_t i = 0; i < right_row.size(); i++)
                    {
                        if (!matching_schema_right.contains(i))
                        {
                            new_row.push_back(right_row[i]);
                        }
                    }
                    rows.push_back(std::move(new_row));
                }
            }
        }
    }

    std::optional<Row> next() override 
    {
        if (pos == rows.size())
        {
            return std::nullopt;
        }

        return rows[pos++];

    }

    std::shared_ptr<Schema> getOutputSchema() override 
    { 
        return schema;
    }

private:
    ExecutorPtr left_input_executor;
    ExecutorPtr right_input_executor;
    std::shared_ptr<Schema> schema;
    Rows rows;
    size_t pos = 0;
};

class GroupByExecutor : public IExecutor
{
public:
    explicit GroupByExecutor(ExecutorPtr input_executor_, GroupByKeys group_by_keys_, GroupByExpressions group_by_expressions_)
        : input_executor(std::move(input_executor_))
        , group_by_keys(std::move(group_by_keys_))
        , group_by_expressions(std::move(group_by_expressions_))
    {
        std::unordered_map<Row, std::unordered_map<std::string, AggregateDataPtr>> values;
        RowSet row_set = shdb::execute(std::move(input_executor));

        for (auto & row : row_set.getRows())
        {
            Row val_key;
            for (auto & key : group_by_keys)
            {
                auto value = key.expression->evaluate(row);
                val_key.push_back(value);
            }
            if (!values.contains(val_key)) {
                values[val_key];
            }
            for (auto & expression : group_by_expressions)
            {
                Row value = {expression.arguments[0]->evaluate(row)};
                if (!values[val_key].contains(expression.aggregate_function_column_name))
                {
                    auto size = expression.aggregate_function->getStateSize();
                    values[val_key][expression.aggregate_function_column_name] = std::shared_ptr<char[]>(new char[size]);
                    expression.aggregate_function->create(values[val_key][expression.aggregate_function_column_name]);
                }
                expression.aggregate_function->add(values[val_key][expression.aggregate_function_column_name], value);
            }
        }
        for (auto key : values)
        {
            Row new_row = key.first;
            for (auto & expression : group_by_expressions)
            {
                auto result = expression.aggregate_function->getResult(values[key.first][expression.aggregate_function_column_name]);
                new_row.push_back(result);
            }
            rows.push_back(std::move(new_row));
        }

        schema = std::make_shared<Schema>(Schema());
        for (auto & key : group_by_keys)
        {
            schema->push_back({key.expression_column_name, key.expression->getResultType()});
        }

        for (auto & expression : group_by_expressions)
        {
            schema->push_back({expression.aggregate_function_column_name, expression.arguments[0]->getResultType()});
        }
    }

    std::optional<Row> next() override 
    {
        if (pos == rows.size())
        {
            return std::nullopt;
        }
        return rows[pos++];
    }

    std::shared_ptr<Schema> getOutputSchema() override 
    {
       return schema;
    }

private:
    ExecutorPtr input_executor;
    GroupByKeys group_by_keys;
    GroupByExpressions group_by_expressions;
    Rows rows;
    size_t pos = 0;
    std::shared_ptr<Schema> schema;
};

}

ExecutorPtr createReadFromRowsExecutor(Rows rows, std::shared_ptr<Schema> rows_schema)
{
    return std::make_unique<ReadFromRowsExecutor>(rows, rows_schema);
}

ExecutorPtr createReadFromTableExecutor(std::shared_ptr<ITable> table, std::shared_ptr<Schema> table_schema)
{
    return std::make_unique<ReadFromTableExecutor>(table, table_schema);
}

ExecutorPtr createExpressionsExecutor(ExecutorPtr input_executor, Expressions expressions)
{
    return std::make_unique<ExpressionsExecutor>(std::move(input_executor), expressions);
}

ExecutorPtr createFilterExecutor(ExecutorPtr input_executor, ExpressionPtr filter_expression)
{
    return std::make_unique<FilterExecutor>(std::move(input_executor), filter_expression);
}

ExecutorPtr createSortExecutor(ExecutorPtr input_executor, SortExpressions sort_expressions)
{
    return std::make_unique<SortExecutor>(std::move(input_executor), sort_expressions);
}

ExecutorPtr createJoinExecutor(ExecutorPtr left_input_executor, ExecutorPtr right_input_executor)
{
    return std::make_unique<JoinExecutor>(std::move(left_input_executor), std::move(right_input_executor));
}

ExecutorPtr createGroupByExecutor(ExecutorPtr input_executor, GroupByKeys group_by_keys, GroupByExpressions group_by_expressions)
{
    return std::make_unique<GroupByExecutor>(std::move(input_executor), group_by_keys, group_by_expressions);
}

RowSet execute(ExecutorPtr executor)
{
    RowSet result;
    while (true)
    {
        auto row = executor->next();
        if (row == std::nullopt)
        {
            break;
        }
        result.addRow(std::move(*row));
    }
    return result;
}

}
