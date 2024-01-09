#include "ast.h"

#include <cassert>

namespace calc {

Ast::Ast(Type type)
    : type(type)
{ }

Number::Number(int value)
    : Ast(Type::number), value(value)
{ }

Binary::Binary(Opcode op, std::shared_ptr<Ast> lhs, std::shared_ptr<Ast> rhs)
    : Ast(Type::binary), op(op), lhs(lhs), rhs(rhs)
{ }

Unary::Unary(Opcode op, std::shared_ptr<Ast> operand)
    : Ast(Type::unary), op(op), operand(operand)
{ }

Assignment::Assignment(std::string name, std::shared_ptr<Ast> expression)
    : Ast(Type::assignment), name(name), expression(expression)
{ }

Variable::Variable(std::string name)
    : Ast(Type::variable), name(name)
{ }

Definition::Definition(std::string name, std::vector<std::string> params, std::vector<std::shared_ptr<Ast>> body)
    : Ast(Type::definition), name(name), params(params), body(body)
{ }

Function::Function(std::string name, std::vector<std::shared_ptr<Ast>> args)
    : Ast(Type::function), name(name), args(args)
{ }

IF::IF(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> body)
    : Ast(Type::if_condition), condition(condition), body(body)
{ }

IF_ELSE::IF_ELSE(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> if_body, std::vector<std::shared_ptr<Ast>> else_body)
    : Ast(Type::if_else_condition), condition(condition), if_body(if_body), else_body(else_body)
{ }

WHILE::WHILE(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> body)
    : Ast(Type::while_condition), condition(condition), body(body)
{ }

Print::Print(std::shared_ptr<Ast> expression) 
    : Ast(Type::print), expression(expression)
{ }

Block::Block(std::vector<std::shared_ptr<Ast>> block) 
    : Ast(Type::block), block(block)
{ }

std::shared_ptr<Ast> new_number(int value)
{
    return std::make_shared<Number>(value);
}

std::shared_ptr<Ast> new_binary(Opcode op, std::shared_ptr<Ast> lhs, std::shared_ptr<Ast> rhs)
{
    return std::make_shared<Binary>(op, lhs, rhs);
}

std::shared_ptr<Ast> new_unary(Opcode op, std::shared_ptr<Ast> operand)
{
    return std::make_shared<Unary>(op, operand);
}

std::shared_ptr<Ast> new_assignment(std::string name, std::shared_ptr<Ast> expression) {
    return std::make_shared<Assignment>(name, expression);
}

std::shared_ptr<Ast> new_variable(std::string name) {
    return std::make_shared<Variable>(name);
}

std::shared_ptr<Ast> new_definition(std::string name, std::vector<std::string> params, std::vector<std::shared_ptr<Ast>> body) {
    return std::make_shared<Definition>(name, params, body);
}

std::shared_ptr<Ast> new_function(std::string name, std::vector<std::shared_ptr<Ast>> args) {
    return std::make_shared<Function>(name, args);
}

std::shared_ptr<Ast> if_condition(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> body) {
    return std::make_shared<IF>(condition, body);
}

std::shared_ptr<Ast> if_else_condition(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> if_body, std::vector<std::shared_ptr<Ast>> else_body) {
    return std::make_shared<IF_ELSE>(condition, if_body, else_body);
}

std::shared_ptr<Ast> while_condition(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> body) {
    return std::make_shared<WHILE>(condition, body);
}

std::shared_ptr<Ast> new_print(std::shared_ptr<Ast> expression) {
    return std::make_shared<Print>(expression);
}

std::shared_ptr<Ast> new_block(std::vector<std::shared_ptr<Ast>> block) {
    return std::make_shared<Block>(block);
}


std::string to_string(std::shared_ptr<Ast> ast)
{
    if (!ast) {
        return {};
    }

    switch (ast->type) {
        case Type::number:
            return std::to_string(std::static_pointer_cast<Number>(ast)->value);
        case Type::binary: {
            auto op = std::static_pointer_cast<Binary>(ast);
            auto get_op = [&] () -> std::string {
                switch (op->op) {
                    case Opcode::plus: return "+";
                    case Opcode::minus: return "-";
                    case Opcode::mul: return "*";
                    case Opcode::div: return "/";
                    case Opcode::AND: return "&&";
                    case Opcode::OR: return "||";
                    case Opcode::eq: return "==";
                    case Opcode::neq: return "!=";
                    case Opcode::bg: return ">";
                    case Opcode::bgq: return ">=";
                    case Opcode::sm: return "<";
                    case Opcode::smq: return "<=";
                    default: assert(0);
                }
            };
            return "(" + to_string(op->lhs) + ") " + get_op() + " (" + to_string(op->rhs) + ")";
        }
        case Type::unary: {
            auto op = std::static_pointer_cast<Unary>(ast);
            auto get_op = [&] () -> std::string {
                switch (op->op) {
                    case Opcode::uminus: return "-";
                    case Opcode::NOT: return "!";
                    default: assert(0);
                }
            };
            return get_op() + "(" + to_string(op->operand) + ")";
        }
        case Type::assignment: {
            auto op = std::static_pointer_cast<Assignment>(ast);
            return op->name + " = " +  to_string(op->expression);
        }
        case Type::variable: {
             auto op = std::static_pointer_cast<Variable>(ast);
             return op->name;
        }
        case Type::definition: {
            auto op = std::static_pointer_cast<Definition>(ast);
            auto str = "def " + op->name + "(";
            for (auto & param : op->params) {
                str += param + ",";
            }
            str += "):do\n";
            for (auto & line : op->body) {
                str += to_string(line) + "\n";
            }
            str += "done";
            return str;
        }
        case Type::function: {
            auto op = std::static_pointer_cast<Function>(ast);
            auto str = op->name + "(";
            for (auto & arg : op->args) {
                str += to_string(arg) + ",";
            }
            str += ")";
            return str;
        }
        case Type::if_condition: {
            auto op = std::static_pointer_cast<IF>(ast);
            std::string str = "if ";
            str += to_string(op->condition) + " do\n";
            for (auto & line : op->body) {
                str += to_string(line) + "\n";
            }
            str += "done";
            return str;
        }
        case Type::if_else_condition: {
            auto op = std::static_pointer_cast<IF_ELSE>(ast);
            std::string str = "if ";
            str += to_string(op->condition) + " do\n";
            for (auto & line : op->if_body) {
                str += to_string(line) + "\n";
            }
            str += "done\n";
            str += "else: do\n";
            for (auto & line : op->else_body) {
                str += to_string(line) + "\n";
            }
            str += "done";
            return str;
        }
        case Type::while_condition: {
            auto op = std::static_pointer_cast<WHILE>(ast);
            std::string str = "while ";
            str += to_string(op->condition) + " do\n";
            for (auto & line : op->body) {
                str += to_string(line) + "\n";
            }
            str += "done";
            return str;
        }
        case Type::print: {
            auto op = std::static_pointer_cast<Print>(ast);
            std::string str = "print(" + to_string(op->expression) + ")";
            return str;
        }
        case Type::block: {
            auto op = std::static_pointer_cast<Block>(ast);
            std::string str = "";
            for (auto ptr : op->block) {
                str += to_string(ptr);
            }
            return str;
        }
        default: {
            assert(0);
        }
    }
}

}
