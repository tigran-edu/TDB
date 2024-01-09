#include "eval.h"

#include <iostream>
#include <cassert>

namespace calc {

int Eval::eval(std::shared_ptr<Ast> ast)
{
    if (!ast) {
        return 0;
    }

    switch (ast->type) {
        case Type::number:
            return std::static_pointer_cast<Number>(ast)->value;
        case Type::binary: {
            auto op = std::static_pointer_cast<Binary>(ast);;
            int lhs = eval(op->lhs);
            int rhs = eval(op->rhs);
            auto get_op = [&] () -> int {
                switch (op->op) {
                    case Opcode::plus: return lhs + rhs;
                    case Opcode::minus: return lhs - rhs;
                    case Opcode::mul: return lhs * rhs;
                    case Opcode::div: return lhs / rhs;
                    case Opcode::AND: return lhs && rhs;
                    case Opcode::OR: return lhs || rhs;;
                    case Opcode::eq: return lhs == rhs;
                    case Opcode::neq: return lhs != rhs;
                    case Opcode::bg: return lhs > rhs;
                    case Opcode::bgq: return lhs >= rhs;
                    case Opcode::sm: return lhs < rhs;
                    case Opcode::smq: return lhs <= rhs;
                    default: assert(0);
                }
            };
            return get_op();
        }
        case Type::unary: {
            auto op = std::static_pointer_cast<Unary>(ast);
            auto get_op = [&] () -> int {
                switch (op->op) {
                    case Opcode::uminus: return -eval(op->operand);
                    case Opcode::NOT: return !eval(op->operand);
                    default: assert(0);
                }
            };
            return get_op();
        }
        case Type::assignment: {
            auto op = std::static_pointer_cast<Assignment>(ast);
            int value = eval(op->expression);
            this->variables[op->name] = new_number(value);
            return value;
        }
        case Type::variable: {
            auto op = std::static_pointer_cast<Variable>(ast);
            return std::static_pointer_cast<Number>(this->variables[op->name])->value;
        }
        case Type::definition: {
            auto op = std::static_pointer_cast<Definition>(ast);
            this->variables[op->name] = ast;
            return 0;
        }
        case Type::function: {
            auto op = std::static_pointer_cast<Function>(ast);
            auto definition = std::static_pointer_cast<Definition>(this->variables[op->name]);
            auto eval = Eval();
            for (int i = 0; i < definition->params.size(); ++i) {
                if (op->args[i]->type == Type::number) {
                    eval.variables[definition->params[i]] = op->args[i];
                } else if (op->args[i]->type == Type::variable) {
                    std::string arg = std::static_pointer_cast<Variable>(op->args[i])->name;
                    eval.variables[definition->params[i]] = this->variables[arg];
                } else {
                    int value = this->eval(op->args[i]);
                    eval.variables[definition->params[i]] = new_number(value);
                }
            }
            int value = 0;
            for (auto & line : definition->body) {
                value = eval.eval(line);
            }
            return value;
        }
        case Type::if_condition: {
            auto op = std::static_pointer_cast<IF>(ast);
            int value = 0;
            if (eval(op->condition)) {
                for (auto & line : op->body) {
                    value = eval(line);
                }
            }
            return value;
        }
        case Type::if_else_condition: {
            auto op = std::static_pointer_cast<IF_ELSE>(ast);
            int value = 0;
            if (eval(op->condition)) {
                for (auto & line : op->if_body) {
                    value = eval(line);
                }
            } else {
                for (auto & line : op->else_body) {
                    value = eval(line);
                }

            }
            return value;
        }
        case Type::while_condition: {
            auto op = std::static_pointer_cast<WHILE>(ast);
            int value = 0;
            while (eval(op->condition)) {
                for (auto & line : op->body) {
                    value = eval(line);
                }
            }
            return value;
        }
        case Type::print: {
            auto op = std::static_pointer_cast<Print>(ast);
            std::cout << eval(op->expression) << "\n";
            return 0;
        }
        case Type::block: {
            auto op = std::static_pointer_cast<Block>(ast);
            int value = 0;
            for (auto ptr : op->block) {
                value = eval(ptr);
            }
            return value;
        }
        default: {
            assert(0);
        }
    }
}

}
