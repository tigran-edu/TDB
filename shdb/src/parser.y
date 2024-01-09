%skeleton "lalr1.cc"
%require "2.5"
%defines
%define api.namespace {shdb}
%define api.value.type variant
%define parser_class_name {Parser}

%code requires {
    #include <memory>
    #include "ast.h"
    #include "schema.h"
    namespace shdb {class Lexer;}
}

%parse-param {shdb::Lexer& lexer} {ASTPtr & result} {std::string & message}

%code {
    #include "lexer.h"
    #define yylex lexer.lex
}

%token PLUS "+"
%token MINUS "-"
%token ALL "*"
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
%token COMMA ","
%token QUOTE '"'


%token END 0 "end of file"
%token ERROR
%token CREATE "CREATE TABLE"
%token DROP "DROP TABLE"
%token INSERT "INSERT"
%token FROM "FROM"
%token WHERE "WHERE"
%token VALUES "VALUES"
%token SELECT "SELECT"
%token ORDER "ORDER BY"
%token GROUP "GROUP BY"
%token HAVING "HAVING"

%token <std::string> MIN "min"
%token <std::string> MAX "max"
%token <std::string> SUM "sum"
%token <std::string> AVG "avg"

%token DESC "DESC"
%token <std::string> NAME
%token <uint32_t> NUM


%type <std::string> TYPE
%type <ASTPtr> row
%type <std::vector<ColumnSchema>> schema
%type <ASTPtr> expr
%type <ASTListPtr> projection

%type <std::vector<std::string>> FROM_POINT
%type <ASTPtr> WHERE_POINT HAVING_POINT
%type <ASTListPtr> GROUP_POINT ORDER_POINT

%left "||"
%left "&&"
%nonassoc "!"
%left "==" "!=" "<" "<=" ">" ">="

%left "+" "-"
%left "*" "/"
%nonassoc UMINUS

%%

input: row { result = $1; }

row: CREATE NAME "(" schema ")" { $$ = newCreateQuery($2, $4); }
    | DROP NAME { $$ = newDropQuery($2); }
    | INSERT NAME VALUES "(" projection ")" { $$ = newInsertQuery($2, $5); }
    | SELECT projection FROM_POINT WHERE_POINT GROUP_POINT HAVING_POINT ORDER_POINT { $$ = newSelectQuery($2, $3, $4, $5, $6, $7); }


FROM_POINT: %empty { $$ = {}; }
    | FROM NAME { $$ = {$2}; }
    | FROM_POINT COMMA NAME { $1.push_back($3); $$ = std::move($1); }

WHERE_POINT: %empty { $$ = nullptr; }
    | WHERE expr { $$ = $2; }

GROUP_POINT: %empty { $$ = std::make_shared<ASTList>(); }
    | GROUP expr { $$ = std::make_shared<ASTList>($2); }
    | GROUP_POINT COMMA expr { $1->append($3); $$ = std::move($1); }

HAVING_POINT: %empty { $$ = nullptr; }
    | HAVING expr { $$ = $2; }

ORDER_POINT: %empty { $$ = nullptr; } 
    | ORDER expr { $$ = newList(newOrder($2, false)); }
    | ORDER expr "DESC" { $$ = newList(newOrder($2, true)); }
    | ORDER_POINT "," expr { $1->append(newOrder($3, false)); $$ = std::move($1); }
    | ORDER_POINT "," expr DESC { $1->append(newOrder($3, true)); $$ = std::move($1); }

schema: NAME TYPE { $$ = {{$1, toType($2)}}; }
    | NAME TYPE "(" NUM ")" { $$ = {{$1, toType($2), $4}}; }
    | schema COMMA NAME TYPE { $1.push_back({$3, toType($4)});  $$ = std::move($1); }
    | schema COMMA NAME TYPE "(" NUM ")" { $1.push_back({$3, toType($4), $6});  $$ = std::move($1); }

expr: NUM { $$ = newNumberLiteral($1); }
    | QUOTE NAME QUOTE { $$ = newStringLiteral($2); }
    | NAME { $$ = newIdentifier($1); }
    | MIN "(" expr ")" {$$ = newFunction("min", newList($3)); }
    | MAX "(" expr ")" {$$ = newFunction("max", newList($3)); }
    | SUM "(" expr ")" {$$ = newFunction("sum", newList($3)); }
    | AVG "(" expr ")" {$$ = newFunction("avg", newList($3)); }
    | ALL { $$ = newIdentifier("*"); }
    | expr "+" expr { $$ = newBinaryOperator(BinaryOperatorCode::plus, $1, $3); }
    | expr "-" expr { $$ = newBinaryOperator(BinaryOperatorCode::minus, $1, $3); }
    | expr "*" expr { $$ = newBinaryOperator(BinaryOperatorCode::mul, $1, $3); }
    | expr "/" expr { $$ = newBinaryOperator(BinaryOperatorCode::div, $1, $3); }
    | "(" expr ")" { $$ = $2; }
    | "-" %prec UMINUS expr { $$ = newUnaryOperator(UnaryOperatorCode::uminus, $2); }
    | "!" "(" expr ")" { $$ = newUnaryOperator(UnaryOperatorCode::lnot, $3); }

    | expr "&&" expr  { $$ = newBinaryOperator(BinaryOperatorCode::land, $1, $3); }
    | expr "||" expr { $$ = newBinaryOperator(BinaryOperatorCode::lor, $1, $3); }

    | expr "==" expr { $$ = newBinaryOperator(BinaryOperatorCode::eq, $1, $3); }
    | expr "!=" expr { $$ = newBinaryOperator(BinaryOperatorCode::ne, $1, $3); }
    | expr ">" expr {$$ = newBinaryOperator(BinaryOperatorCode::gt, $1, $3); }
    | expr ">=" expr { $$ = newBinaryOperator(BinaryOperatorCode::ge, $1, $3); }
    | expr "<" expr { $$ = newBinaryOperator(BinaryOperatorCode::lt, $1, $3); }
    | expr "<=" expr { $$ = newBinaryOperator(BinaryOperatorCode::le, $1, $3); }


projection: expr { $$ = newList($1); }
    | projection "," expr { $1->append($3); $$ = std::move($1); }
;

TYPE: NAME { $$ = $1; }

%%

void shdb::Parser::error(const std::string& err)
{
	message = err;
}
