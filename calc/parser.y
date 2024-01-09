%skeleton "lalr1.cc"
%require "2.5"
%defines
%define api.namespace {calc}
%define api.value.type variant
%define parser_class_name {Parser}

%code requires {
    #include <iostream>
    #include <memory>
    #include "ast.h"
    namespace calc {class Lexer;}
}

%parse-param {calc::Lexer& lexer} {std::shared_ptr<Ast>& result} {std::string& message}

%code {
    #include "lexer.h"
    #define yylex lexer.lex
}

%token END 0 "end of file"
%token ERROR
%token EOL "\n"

%token <int> NUM
%token <std::string> NAME

%token PLUS "+"
%token MINUS "-"
%token MUL "*"
%token DIV "/"
%token LPAR "("
%token RPAR ")"
%token ASGM "="
%token AND "&&"
%token OR "||"
%token NOT "!"
%token EQ "=="
%token NEQ "!="
%token SM "<"
%token SMQ "<="
%token BG ">"
%token BGQ ">="
%token FUNC "def"
%token DO "do"
%token DONE "done"
%token IF "if"
%token ELSE "else"
%token WHILE "while"
%token PRINT "print"
%token COMMA ","
%token COLON ":"


%type <std::shared_ptr<Ast>> expr stmt
%type <std::vector<std::string>> params
%type <std::vector<std::shared_ptr<Ast>>> args block

%left "||"
%left "&&"
%nonassoc "!"
%left "==" "!=" "<" "<=" ">" ">="

%left "+" "-"
%left "*" "/"
%nonassoc UMINUS

%precedence "then"
%precedence "else"
%%

input: block { result = new_block($1); }

expr: NUM { $$ = new_number($1); }
    | NAME { $$ = new_variable($1); }
    | expr "+" expr { $$ = new_binary(Opcode::plus, $1, $3); }
    | expr "-" expr { $$ = new_binary(Opcode::minus, $1, $3); }
    | expr "*" expr { $$ = new_binary(Opcode::mul, $1, $3); }
    | expr "/" expr { $$ = new_binary(Opcode::div, $1, $3); }
    | "(" expr ")" { $$ = $2; }
    | "-" %prec UMINUS expr { $$ = new_unary(Opcode::uminus, $2); }
    | "!" "(" expr ")" { $$ = new_unary(Opcode::NOT, $3); }

    | expr "&&" expr  { $$ = new_binary(Opcode::AND, $1, $3); }
    | expr "||" expr { $$ = new_binary(Opcode::OR, $1, $3); }

    | expr "==" expr { $$ = new_binary(Opcode::eq, $1, $3); }
    | expr "!=" expr { $$ = new_binary(Opcode::neq, $1, $3); }
    | expr ">" expr {$$ = new_binary(Opcode::bg, $1, $3); }
    | expr ">=" expr { $$ = new_binary(Opcode::bgq, $1, $3); }
    | expr "<" expr { $$ = new_binary(Opcode::sm, $1, $3); }
    | expr "<=" expr { $$ = new_binary(Opcode::smq, $1, $3); }
;

stmt: expr "\n" { $$ = $1; }
    | NAME "=" stmt { $$ = new_assignment($1, $3); }
    | NAME "(" args ")" "\n" { $$ = new_function($1, $3); }
    | "def" NAME "(" params ")" ":" expr "\n" { $$ = new_definition($2, $4, {$7}); }
    | "def" NAME "(" params ")" ":" "do" "\n" block "done" "\n" { $$ = new_definition($2, $4, $9); }
    | "print" "(" expr ")" "\n" { $$ = new_print($3); }

    | "if" expr ":" "do" "\n" block "done" "\n" %prec "then" {$$ = if_condition($2, $6); }
    | "if" expr ":" "do" "\n" block "done" "\n" "else" ":" "do" "\n" block "done" "\n" {$$ = if_else_condition($2, $6, $13); }
    | "while" expr ":" "do" "\n" block "done" "\n" {$$ = while_condition($2, $6); }

;

params:
    | NAME { $$ = {$1}; }
    | params "," NAME { $1.push_back($3); $$ = std::move($1); }
;

args: 
    | expr { $$ = {$1}; }
    | args "," expr { $1.push_back($3); $$ = std::move($1); }
;

block: stmt { $$ = {$1}; }
    | block stmt {$1.push_back($2); $$ = std::move($1); }
;

%%

void calc::Parser::error(const std::string& err)
{
    message = err;
}
