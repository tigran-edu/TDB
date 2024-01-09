#include "lexer.h"

namespace calc {

%%{
    machine lexer;
    main := |*
        digit+ => {
            ret = Parser::token::NUM;
            Parser::semantic_type num(strtol(std::string(ts, te).c_str(), 0, 10));
            val->move<int>(num);
            fbreak;
        };

        'def' => {ret = Parser::token::FUNC; fbreak; };
        'do' => {ret = Parser::token::DO; fbreak; };
        'done' => {ret = Parser::token::DONE; fbreak; };
        'if' => {ret = Parser::token::IF; fbreak; };
        'else' => {ret = Parser::token::ELSE; fbreak; };
        'while' => {ret = Parser::token::WHILE; fbreak; };
        'print' => {ret = Parser::token::PRINT; fbreak; };
        ',' => { ret = Parser::token::COMMA; fbreak; };
        ':' => { ret = Parser::token::COLON; fbreak; };

        alpha.alnum* => {
            ret = Parser::token::NAME;
            Parser::semantic_type str(std::string(ts, te));
            val->move<std::string>(str);
            fbreak;
        };

        '+' => { ret = Parser::token::PLUS; fbreak; };
        '-' => { ret = Parser::token::MINUS; fbreak; };
        '*' => { ret = Parser::token::MUL; fbreak; };
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


        '\n' => { ret = Parser::token::EOL; fbreak; };

        '\t' => { fbreak; };


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
