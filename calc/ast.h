#pragma once

#include <string>
#include <memory>
#include <iostream>
#include <vector>

namespace calc {

enum class Type {
    number,
    binary,
    unary,
    block,
    assignment,
    variable,
    definition,
    function,
    if_condition,
    if_else_condition,
    while_condition,
    print
};

enum class Opcode {
    plus,
    minus,
    mul,
    div,
    uminus,
    eq,
    neq,
    bg,
    bgq,
    sm,
    smq,
    AND,
    OR,
    NOT,
};


struct Ast
{
    Type type;

    Ast(Type type);
    virtual ~Ast() = default;
};

struct Number
    : public Ast
{
    int value;

    explicit Number(int value);
};

struct Binary
    : public Ast
{
    Opcode op;
    std::shared_ptr<Ast> lhs;
    std::shared_ptr<Ast> rhs;

    Binary(Opcode op, std::shared_ptr<Ast> lhs, std::shared_ptr<Ast> rhs);
};

struct Unary
    : public Ast
{
    Opcode op;
    std::shared_ptr<Ast> operand;

    Unary(Opcode op, std::shared_ptr<Ast> operand);
};

struct Assignment
    : public Ast
{
    std::string name;
    std::shared_ptr<Ast> expression;

    Assignment(std::string name, std::shared_ptr<Ast> expression);
};

struct Variable
    : public Ast
{
    std::string name;

    Variable(std::string name);
};

struct Definition
    : public Ast
{
    std::string name;
    std::vector<std::string> params;
    std::vector<std::shared_ptr<Ast>> body;

    Definition(std::string name, std::vector<std::string> params, std::vector<std::shared_ptr<Ast>> body);
};

struct Function
    : public Ast
{
    std::string name;
    std::vector<std::shared_ptr<Ast>> args;

    Function(std::string name, std::vector<std::shared_ptr<Ast>> args);
};

struct IF 
    : public Ast
{
    std::shared_ptr<Ast> condition;
    std::vector<std::shared_ptr<Ast>> body;

    IF(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> body);
};

struct IF_ELSE
    : public Ast
{
    std::shared_ptr<Ast> condition;
    std::vector<std::shared_ptr<Ast>> if_body;
    std::vector<std::shared_ptr<Ast>> else_body;

    IF_ELSE(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> if_body, std::vector<std::shared_ptr<Ast>> else_body);
};

struct WHILE
    : public Ast
{
    std::shared_ptr<Ast> condition;
    std::vector<std::shared_ptr<Ast>> body;

    WHILE(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> body);
};

struct Print
    : public Ast
{
    std::shared_ptr<Ast> expression;

    Print(std::shared_ptr<Ast> expression);
};

struct Block
    : public Ast
{
    std::vector<std::shared_ptr<Ast>> block;

    Block(std::vector<std::shared_ptr<Ast>> block);
};


std::shared_ptr<Ast> new_number(int value);
std::shared_ptr<Ast> new_binary(Opcode op, std::shared_ptr<Ast> lhs, std::shared_ptr<Ast> rhs);
std::shared_ptr<Ast> new_unary(Opcode op, std::shared_ptr<Ast> operand);
std::shared_ptr<Ast> new_assignment(std::string name, std::shared_ptr<Ast> expression);
std::shared_ptr<Ast> new_variable(std::string name);
std::shared_ptr<Ast> new_definition(std::string name, std::vector<std::string> params, std::vector<std::shared_ptr<Ast>> body);
std::shared_ptr<Ast> new_function(std::string name, std::vector<std::shared_ptr<Ast>> args);
std::shared_ptr<Ast> if_condition(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> body);
std::shared_ptr<Ast> if_else_condition(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> if_body, std::vector<std::shared_ptr<Ast>> else_body);
std::shared_ptr<Ast> while_condition(std::shared_ptr<Ast> condition, std::vector<std::shared_ptr<Ast>> body);
std::shared_ptr<Ast> new_print(std::shared_ptr<Ast> expression);
std::shared_ptr<Ast> new_block(std::vector<std::shared_ptr<Ast>> block);

std::string to_string(std::shared_ptr<Ast> ast);

}
