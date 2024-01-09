#include "parser.hh"
#include "lexer.h"
#include "ast.h"
#include "eval.h"
#include <iostream>
#include <string>

#include <unistd.h>

int main()
{
    calc::Eval eval;
    std::string input;

    std::string error;
    int result = 0;
    while (!std::cin.eof()) {
        if (isatty(0)) {
            std::cout << (input.empty() ? "> " : ". ");
        }
        std::string line;
        std::getline(std::cin, line);
        input += line;
        if (input.empty()) {
            continue;
        }
        input += '\n';
        calc::Lexer lexer(input.c_str(), input.c_str() + input.size());
        std::shared_ptr<calc::Ast> ast;
        calc::Parser parser(lexer, ast, error);
        parser.parse();
        if (ast) {
            // std::cout << "====== AST ======" <<'\n';
            // std::cout << to_string(ast) << '\n';
            // std::cout << "=================" << '\n' << std::endl;
            result = eval.eval(ast);
            if (isatty(0)) {
                std::cout << result << std::endl;
            }
            input.clear();
            error.clear();
        }
    }
    if (!error.empty()) {
        std::cout << "Parse error: " << error << std::endl;
    } else if (!isatty(0)) {
        std::cout << result << std::endl;
    }
}
