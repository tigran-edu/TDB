#include "lexer.h"

namespace shdb {

%%{
    machine lexer;
    main := |*
        digit+ => {
            ret = Parser::token::NUM;
            Parser::semantic_type num(strtol(std::string(ts, te).c_str(), 0, 10));
            val->move<uint32_t>(num);
            fbreak;
        };

        'CREATE TABLE' => {ret = Parser::token::CREATE; fbreak; };
        'DROP TABLE' => {ret = Parser::token::DROP; fbreak; };
        'SELECT' => {ret = Parser::token::SELECT; fbreak; };
        'INSERT' => {ret = Parser::token::INSERT; fbreak; };
        'VALUES' => {ret = Parser::token::VALUES; fbreak; };
        'FROM' => {ret = Parser::token::FROM; fbreak; };
        'WHERE' => {ret = Parser::token::WHERE; fbreak; };
        'ORDER BY' => { ret = Parser::token::ORDER; fbreak; };
        'GROUP BY' => {ret = Parser::token::GROUP; fbreak; };
        'HAVING' => {ret = Parser::token::HAVING; fbreak; };
        'DESC' => { ret = Parser::token::DESC; fbreak; };

        'min' => {ret = Parser::token::MIN; fbreak; };
        'max' => {ret = Parser::token::MAX; fbreak; };
        'avg' => {ret = Parser::token::AVG; fbreak; };
        'sum' => {ret = Parser::token::SUM; fbreak; };

        '+' => { ret = Parser::token::PLUS; fbreak; };
        '-' => { ret = Parser::token::MINUS; fbreak; };
        '*' => { ret = Parser::token::ALL; fbreak; };
        '/' => { ret = Parser::token::DIV; fbreak; };
        '(' => { ret = Parser::token::LPAR; fbreak; };
        ')' => { ret = Parser::token::RPAR; fbreak; };
        '=' => { ret = Parser::token::ASGM; fbreak; };

        '&&' => { ret = Parser::token::AND; fbreak; };
        '||' => { ret = Parser::token::OR; fbreak; };
        '!' => { ret = Parser::token::NOT; fbreak; };

        '==' => { ret = Parser::token::EQ; fbreak; };
        '!=' => { ret = Parser::token::NEQ; fbreak; };
        '>' => { ret = Parser::token::BG; fbreak; };
        '>=' => { ret = Parser::token::BGQ; fbreak; };
        '<' => { ret = Parser::token::SM; fbreak; };
        '<=' => { ret = Parser::token::SMQ; fbreak; };
    
        ',' => { ret = Parser::token::COMMA; fbreak; };
        '"' => { ret = Parser::token::QUOTE; fbreak; };

        alpha (alnum | '_')* => {
            ret = Parser::token::NAME;
            Parser::semantic_type str(std::string(ts, te));
            val->move<std::string>(str);
            fbreak;
        };


        space;
    *|;
    write data;
}%%


Lexer::Lexer(const char *p, const char *pe)
    : p(p), pe(pe), eof(pe)
{
    %%write init;
}

Parser::token_type Lexer::lex(Parser::semantic_type* val)
{
    Parser::token_type ret = Parser::token::END;
    %%write exec;

    if (ret == Parser::token::END && p != pe && te != pe) {
        std::cerr << "Unexpected input: \"" << std::string(te, pe) << "\"" << std::endl;
        ret = Parser::token::ERROR;
    }

    return ret;
}

}
