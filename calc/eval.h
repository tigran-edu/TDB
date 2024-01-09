#pragma once

#include "ast.h"

#include <unordered_map>
#include <variant>

namespace calc {

class Eval
{
public:
    Eval() = default;

    int eval(std::shared_ptr<Ast> stmt);
    std::unordered_map<std::string, std::shared_ptr<Ast>> variables;
};

}
