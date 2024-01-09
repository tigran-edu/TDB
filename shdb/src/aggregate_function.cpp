#include "aggregate_function.h"
#include "limits.h"
#include "iostream"

namespace shdb
{

namespace
{

enum SimpleAggregateFunctionType
{
    min,
    max,
    sum,
    avg
};

template <SimpleAggregateFunctionType AggregateFunctionType>
class SimpleAggregateFunction : public IAggregateFunction
{
public:
    explicit SimpleAggregateFunction(const std::vector<Type> & argument_types_)
        : IAggregateFunction(argument_types_), result_type(Type::int64)
    {}

    Type getResultType() override { return result_type; }

    size_t getStateSize() override 
    {  
        if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::avg)
        {
            return 2 * sizeof(int64_t);
        }
        return sizeof(int64_t);
    }

    void create(AggregateDataPtr place) override
    {
        auto data = reinterpret_pointer_cast<int64_t[]>(place);
        if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::max)
        {
            data[0] = LLONG_MIN;
        }
        else if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::min)
        {
            data[0] = LLONG_MAX;
        }
        else if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::avg)
        {
            data[0] = 0;
            data[1] = 0;
        }
        else if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::sum)
        {
            data[0] = 0;
        }
    }

    void destroy(AggregateDataPtr place) override
    {
        (void)(place);
        throw std::runtime_error("Not implemented");
    }

    void add(AggregateDataPtr place, Row arguments) override
    {
        auto data = std::reinterpret_pointer_cast<int64_t[]>(place);
        if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::max)
        {
            data[0] = std::max(data[0], std::get<int64_t>(arguments[0]));
        }
        else if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::min)
        {
           data[0] = std::min(data[0], std::get<int64_t>(arguments[0]));
        }
        else if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::avg)
        {
            data[0] += std::get<int64_t>(arguments[0]);
            data[1] += 1;
        }
        else if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::sum)
        {
            data[0] += std::get<int64_t>(arguments[0]);
        }
    }

    Value getResult(AggregateDataPtr place) override
    {
        auto data = std::reinterpret_pointer_cast<int64_t[]>(place);
        if constexpr (AggregateFunctionType == SimpleAggregateFunctionType::avg)
        {
            return Value(data[0] / data[1]);
        }
        return Value(data[0]);
    }

private:
    Type result_type;
};

}

AggregateFunctionPtr
AggregateFunctionFactory::getAggregateFunctionOrNull(const std::string & aggregate_function_name, const Types & argument_types)
{
    if (aggregate_function_name_to_create_callback.contains(aggregate_function_name))
    {
        return aggregate_function_name_to_create_callback.at(aggregate_function_name)(argument_types);
    }
    return nullptr;
}

AggregateFunctionPtr
AggregateFunctionFactory::getAggregateFunctionOrThrow(const std::string & aggregate_function_name, const Types & argument_types)
{
    if (aggregate_function_name_to_create_callback.contains(aggregate_function_name))
    {
        return aggregate_function_name_to_create_callback.at(aggregate_function_name)(argument_types);
    }
    throw std::runtime_error("Unknown aggregate function: " + aggregate_function_name);
}

void AggregateFunctionFactory::registerAggregateFunction(
    const std::string & aggregate_function_name, AggregateFunctionCreateCallback create_callback)
{
    aggregate_function_name_to_create_callback[aggregate_function_name] = create_callback;
}

void registerAggregateFunctions(AggregateFunctionFactory & aggregate_function_factory)
{
    aggregate_function_factory.registerAggregateFunction("min", [](const Types & argument_types) { return std::make_shared<SimpleAggregateFunction<SimpleAggregateFunctionType::min>>(std::move(argument_types));});
    aggregate_function_factory.registerAggregateFunction("max", [](const Types & argument_types) { return std::make_shared<SimpleAggregateFunction<SimpleAggregateFunctionType::max>>(std::move(argument_types));});
    aggregate_function_factory.registerAggregateFunction("sum", [](const Types & argument_types) { return std::make_shared<SimpleAggregateFunction<SimpleAggregateFunctionType::sum>>(std::move(argument_types));});
    aggregate_function_factory.registerAggregateFunction("avg", [](const Types & argument_types) { return std::make_shared<SimpleAggregateFunction<SimpleAggregateFunctionType::avg>>(std::move(argument_types));});
}

}
