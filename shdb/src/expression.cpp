#include "expression.h"
#include "comparator.h"
#include "iostream"

namespace shdb
{

namespace
{

class IdentifierExpression : public IExpression
{
public:
    explicit IdentifierExpression(const std::string & identifier_name, const std::shared_ptr<SchemaAccessor> & input_schema_accessor) 
    : identifier_name(identifier_name), input_schema_accessor(input_schema_accessor)
    {}

    Type getResultType() override 
    {
        return input_schema_accessor->getColumnOrThrow(identifier_name).type;
    }

    Value evaluate(const Row & input_row) override
    {
        auto pos = input_schema_accessor->getColumnIndexOrThrow(identifier_name);
        auto value = input_row[pos];
        return value;
    }
private:
    std::string identifier_name;
    std::shared_ptr<SchemaAccessor> input_schema_accessor;
};

class NumberConstantExpression : public IExpression
{
public:
    explicit NumberConstantExpression(int64_t value_) : value(value_) { }

    Type getResultType() override { return Type::int64; }

    Value evaluate(const Row &) override { return value; }

    Value value;
};

class StringConstantExpression : public IExpression
{
public:
    explicit StringConstantExpression(std::string value_) : value(value_) { }

    Type getResultType() override { return Type::string; }

    Value evaluate(const Row &) override { return value; }

    Value value;
};

class BinaryOperatorExpression : public IExpression
{
public:
    explicit BinaryOperatorExpression(
        BinaryOperatorCode binary_operator_code_, ExpressionPtr lhs_expression_, ExpressionPtr rhs_expression_)
        : binary_operator_code(binary_operator_code_)
        , lhs_expression(std::move(lhs_expression_))
        , rhs_expression(std::move(rhs_expression_))
        , lhs_type(lhs_expression->getResultType())
        , rhs_type(rhs_expression->getResultType())
    {}

    Type getResultType() override 
    {
        switch (binary_operator_code)
        {
        case BinaryOperatorCode::plus: return Type::int64;
        case BinaryOperatorCode::minus: return Type::int64;
        case BinaryOperatorCode::mul: return Type::int64;
        case BinaryOperatorCode::div: return Type::int64;
        case BinaryOperatorCode::eq: return Type::boolean;
        case BinaryOperatorCode::ne: return Type::boolean;
        case BinaryOperatorCode::lt: return Type::boolean;
        case BinaryOperatorCode::le: return Type::boolean;
        case BinaryOperatorCode::gt: return Type::boolean;
        case BinaryOperatorCode::ge: return Type::boolean;
        case BinaryOperatorCode::land: return Type::boolean;
        case BinaryOperatorCode::lor: return Type::boolean;
        default:
            throw std::runtime_error("Unsupported operator");
        }
    }

    Value evaluate(const Row & input_row) override
    {
        switch (binary_operator_code)
        {
        case BinaryOperatorCode::plus:
            return std::get<int64_t>(lhs_expression->evaluate(input_row)) + std::get<int64_t>(rhs_expression->evaluate(input_row));
        case BinaryOperatorCode::minus:
            return std::get<int64_t>(lhs_expression->evaluate(input_row)) - std::get<int64_t>(rhs_expression->evaluate(input_row));
        case BinaryOperatorCode::mul:
            return std::get<int64_t>(lhs_expression->evaluate(input_row)) * std::get<int64_t>(rhs_expression->evaluate(input_row));
        case BinaryOperatorCode::div:
            return std::get<int64_t>(lhs_expression->evaluate(input_row)) / std::get<int64_t>(rhs_expression->evaluate(input_row));
        case BinaryOperatorCode::eq: 
            return compareValue(lhs_expression->evaluate(input_row), rhs_expression->evaluate(input_row)) == 0;
        case BinaryOperatorCode::ne:
            return compareValue(lhs_expression->evaluate(input_row), rhs_expression->evaluate(input_row)) != 0;
        case BinaryOperatorCode::lt:
            return compareValue(lhs_expression->evaluate(input_row), rhs_expression->evaluate(input_row)) == -1;
        case BinaryOperatorCode::le:
            return compareValue(lhs_expression->evaluate(input_row), rhs_expression->evaluate(input_row)) <= 0;
        case BinaryOperatorCode::gt:
            return compareValue(lhs_expression->evaluate(input_row), rhs_expression->evaluate(input_row)) == 1;
        case BinaryOperatorCode::ge:
            return compareValue(lhs_expression->evaluate(input_row), rhs_expression->evaluate(input_row)) >= 0;
        case BinaryOperatorCode::land:
            return std::get<bool>(lhs_expression->evaluate(input_row)) && std::get<bool>(rhs_expression->evaluate(input_row));
        case BinaryOperatorCode::lor:
            return std::get<bool>(lhs_expression->evaluate(input_row)) || std::get<bool>(rhs_expression->evaluate(input_row));
        default:
            throw std::runtime_error("Unsupported operator");
        }
    }

    const BinaryOperatorCode binary_operator_code;
    ExpressionPtr lhs_expression;
    ExpressionPtr rhs_expression;
    Type lhs_type;
    Type rhs_type;
};

class UnaryOperatorExpression : public IExpression
{
public:
    explicit UnaryOperatorExpression(UnaryOperatorCode unary_operator_code_, ExpressionPtr expression_)
        : unary_operator_code(unary_operator_code_), expression(std::move(expression_)), expression_type(expression->getResultType())
    {}

    Type getResultType() override 
    {
        switch (unary_operator_code)
        {
        case UnaryOperatorCode::lnot:
            return Type::boolean;
        case UnaryOperatorCode::uminus:
            return Type::int64;
        default:
            throw std::runtime_error("Unsupported unary operator");
        }
    }

    Value evaluate(const Row & input_row) override
    {
        auto result = expression->evaluate(input_row);
        switch (unary_operator_code)
        {
        case UnaryOperatorCode::lnot:
            return !std::get<bool>(result);
        case UnaryOperatorCode::uminus:
            return -std::get<int64_t>(result);
        default:
            throw std::runtime_error("Unsupported unary operator");
        }
    }

    const UnaryOperatorCode unary_operator_code;
    ExpressionPtr expression;
    Type expression_type;
};

}

ExpressionPtr buildExpression(const ASTPtr & expression, const std::shared_ptr<SchemaAccessor> & input_schema_accessor)
{
    switch (expression->type)
    {
    case ASTType::literal:
    {
        auto literal_expression = std::reinterpret_pointer_cast<ASTLiteral>(expression);
        ASTLiteralType literal_type = literal_expression->literal_type;
        if (literal_type == ASTLiteralType::number)
        {
            return std::make_shared<NumberConstantExpression>(literal_expression->integer_value);
        }
        if (literal_type == ASTLiteralType::string)
        {
            return std::make_shared<StringConstantExpression>(literal_expression->string_value);
        }
        break;
    }
    case ASTType::binaryOperator:
    {
        auto binary_operator_expression = std::reinterpret_pointer_cast<ASTBinaryOperator>(expression);
        auto lhs_expression = buildExpression(binary_operator_expression->getLHS(), input_schema_accessor);
        auto rhs_expression = buildExpression(binary_operator_expression->getRHS(), input_schema_accessor);
        return std::make_shared<BinaryOperatorExpression>(binary_operator_expression->operator_code, lhs_expression, rhs_expression);
    }
    case ASTType::unaryOperator:
    {
        auto unary_operator_expression = std::reinterpret_pointer_cast<ASTUnaryOperator>(expression);
        auto expression = buildExpression(unary_operator_expression->getOperand(), input_schema_accessor);
        return std::make_shared<UnaryOperatorExpression>(unary_operator_expression->operator_code, expression);
    }
    case ASTType::identifier:
    {
        auto identifier_expression = std::reinterpret_pointer_cast<ASTIdentifier>(expression);
        return std::make_shared<IdentifierExpression>(identifier_expression->name, input_schema_accessor);
    }
    case ASTType::function:
    {
        auto function_expression = std::reinterpret_pointer_cast<ASTFunction>(expression);
        auto function_name = function_expression->getName();
        return std::make_shared<IdentifierExpression>(function_name, input_schema_accessor);
    }
    default:
        throw std::runtime_error("Undifined operator expression");
    }
}

Expressions buildExpressions(const ASTs & expressions, const std::shared_ptr<SchemaAccessor> & input_schema_accessor)
{
    Expressions answer;
    for (const auto & expression : expressions)
    {
        if (input_schema_accessor != nullptr && input_schema_accessor->hasColumn(expression->getName()))
        {
            std::string name = expression->getName();
            auto new_col = std::make_shared<ASTIdentifier>(ASTIdentifier(name));
            answer.push_back(buildExpression(new_col, input_schema_accessor));
        }
        else 
        {
           answer.push_back(buildExpression(expression, input_schema_accessor));
        }
    }
    return answer;
}

}
