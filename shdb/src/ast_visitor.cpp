#include "ast_visitor.h"
#include "iostream"

namespace shdb
{

namespace
{

class CollectAggregateFunctionsVisitor : public ASTVisitor<CollectAggregateFunctionsVisitor>
{
public:
    explicit CollectAggregateFunctionsVisitor(AggregateFunctionFactory & factory_) : factory(factory_) { }

    void visitImpl(const ASTPtr & node)
    {
        if (node->type == ASTType::function)
        {
            auto function = reinterpret_pointer_cast<ASTFunction>(node);
            if (factory.getAggregateFunctionOrNull(function->name, {Type::int64}) != nullptr)
            {
                aggregate_functions.push_back(node);
            }
        }   
    }
    AggregateFunctionFactory & factory;
    ASTs aggregate_functions;
};

}

ASTs collectAggregateFunctions(const ASTs & expressions, AggregateFunctionFactory & factory)
{
    auto visitor = CollectAggregateFunctionsVisitor(factory);

    for (auto & expression : expressions)
    {
        if (expression != nullptr)
        {
            visitor.visit(expression);
        }
    }
    return visitor.aggregate_functions;
}

}
