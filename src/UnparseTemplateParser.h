/*
 * UnparseTemplateParser.h
 *
 *  Created on: Oct 5, 2015
 *      Author: jrivers
 */

#ifndef SRC_UNPARSETEMPLATEPARSER_H_
#define SRC_UNPARSETEMPLATEPARSER_H_

#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>

#include <string>
#include <vector>
#include <ctype.h>

namespace scidb
{
    using namespace std;

    class TemplateScanner
    {
        std::string format;
        std::string ident;
        size_t pos;
        int    num;

      public:
        enum Token {
            TKN_EOF,
            TKN_IDENT,
            TKN_LPAR,
            TKN_RPAR,
            TKN_COMMA,
            TKN_NUMBER
        };

        std::string const& getIdent() const
        {
            return ident;
        }

        int getNumber() const
        {
            return num;
        }

        size_t getPosition() const
        {
            return pos;
        }

        TemplateScanner(string const& fmt) : format(fmt), pos(0) {}

        Token get()
        {
            int ch = 0;

            while (pos < format.size() && isspace(ch = format[pos])) {
                pos += 1; // ignore whitespaces
            }
            if (pos == format.size()) {
                return TKN_EOF;
            }

            switch (ch) {
              case '(':
                pos += 1;
                return TKN_LPAR;
              case ')':
                pos += 1;
                return TKN_RPAR;
              case ',':
                pos += 1;
                return TKN_COMMA;
              default:
                if (isdigit(ch)) {
                    num = 0;
                    do {
                        pos += 1;
                        num = num*10 + ch - '0';
                    } while (pos < format.size() && isdigit(ch = format[pos]));
                    return TKN_NUMBER;
                } else if (isalpha(ch)) {
                    ident.clear();
                    do {
                        pos += 1;
                        ident += (char)ch;
                    } while (pos < format.size() && (isalnum(ch = format[pos]) || ch == '_'));
                    return TKN_IDENT;
                } else {
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_TEMPLATE_PARSE_ERROR) << pos;
                }
            }
        }
    };

    struct ExchangeTemplate
    {
        struct Column {
            bool skip;
            bool nullable;
            Type internalType;
            Type externalType;
            size_t fixedSize;
            FunctionPointer converter;
        };
        std::vector<Column> columns;
        bool opaque;
    };

    class TemplateParser
    {
      public:
        static ExchangeTemplate parse(ArrayDesc const& desc, std::string const& format, bool isImport);
    };
}


#endif /* SRC_UNPARSETEMPLATEPARSER_H_ */
