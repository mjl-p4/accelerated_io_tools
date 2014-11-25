/*
**
* BEGIN_COPYRIGHT
*
* load_tools is a plugin for SciDB.  Copyright (C) 2008-2014 SciDB, Inc.
*
* load_tools is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* load_tools is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with load_tools.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <vector>

#include <boost/lexical_cast.hpp>
#include <boost/assign.hpp>

#include "query/FunctionLibrary.h"
#include "query/FunctionDescription.h"
#include "system/ErrorsLibrary.h"


using namespace std;
using namespace boost;
using namespace boost::assign;
using namespace scidb;


/**
 * DCAST: cast with default, does not throw an error. Tries to cast input to the appropriate type. If the cast fails,
 * returns the supplied default.
 */
template <typename T>
static void dcast(const Value** args, Value *res, void*)
{
  if(args[0]->isNull())
  {
      res->setNull(args[0]->getMissingReason());
      return;
  }
  const char* s = args[0]->getString();
  try
  {
      T result = lexical_cast<T>(s);
      res->set<T>(result);
  }
  catch(...)
  {
      Value const* def = args[1];
      if(def->isNull())
      {
          res->setNull( def->getMissingReason());
      }
      else
      {
          res->set<T>(def->get<T>());
      }
  }
}

static scidb::UserDefinedFunction dcast_uint64 (scidb::FunctionDescription("dcast", list_of("string")("uint64"), "uint64", &dcast<uint64_t> ));
static scidb::UserDefinedFunction dcast_int64  (scidb::FunctionDescription("dcast", list_of("string")("int64"),  "int64" , &dcast<int64_t>));
static scidb::UserDefinedFunction dcast_double (scidb::FunctionDescription("dcast", list_of("string")("double"), "double", &dcast<double> ));

/**
 * first argument: the string to trim
 * second argument: a set of characters to trim, represented as another string {S}
 * returns: the first argument with all occurrences of any of the characters in S removed from the beginning or end of the string
 */
static void trim (const Value** args, Value *res, void*)
{
    if(args[0]->isNull())
    {
      res->setNull(args[0]->getMissingReason());
      return;
    }
    const char* input = args[0]->getString();
    size_t inputLen = args[0]->size();
    const char* chars = args[1]->getString();
    size_t charsLen = args[1]->size();
    if (charsLen == 0 || (charsLen ==1 && chars[0]==0))
    {
        res->setSize(inputLen);
        memcpy(res->data(), input, inputLen);
        return;
    }
    const char* start = input;
    for(size_t i=0; i<inputLen; ++i)
    {
        char ch = input[i];
        if(ch==0)
        {
            break;
        }
        bool match = false;
        for(size_t j=0; j<charsLen; ++j)
        {
            if(ch == chars[j])
            {
                match =true;
                break;
            }
        }
        if(!match)
        {
            break;
        }
        ++start;
    }
    const char* end = input + inputLen - 1;
    for(ssize_t i = inputLen-1; i>=0; --i)
    {
        char ch = input[i];
        if(ch==0)
        {
            continue;
        }
        bool match = false;
        for(size_t j=0; j<charsLen; ++j)
        {
            if(ch == chars[j])
            {
                match =true;
                break;
            }
        }
        if(!match)
        {
            break;
        }
        --end;
    }
    if (inputLen == 0 || (inputLen ==1 && input[0]==0) || start >= end)
    {
        res->setSize(1);
        ((char *) res->data())[0]=0;
        return;
    }
    size_t size = end - start + 1;
    res->setSize(size);
    memcpy(res->data(), start, (end-start));
    ((char*)res->data())[size-1]=0;
}

static scidb::UserDefinedFunction trim_str (scidb::FunctionDescription("trim", list_of("string")("string"), "string", &trim ));


static void int_to_char (const Value** args, Value *res, void*)
{
    if(args[0]->isNull())
    {
      res->setNull(args[0]->getMissingReason());
      return;
    }
    uint8_t input = args[0]->getUint8();
    res->setChar(input);
}

static scidb::UserDefinedFunction int_to_c (scidb::FunctionDescription("int_to_char", list_of("uint8"), "char", &int_to_char ));
static scidb::UserDefinedFunction c_to_int (scidb::FunctionDescription("char_to_int", list_of("char"), "uint8", &int_to_char ));

static void codify (const Value** args, Value *res, void*)
{
    if(args[0]->isNull())
    {
      res->setNull(args[0]->getMissingReason());
      return;
    }
    const char* input = args[0]->getString();
    size_t inputLen = args[0]->size();
    ostringstream out;
    for (size_t i=0; i< inputLen; ++i)
    {
        char c = input[i];
        int32_t res = c;
        out<<res<<"|";
    }
    res->setString(out.str().c_str());
}

static scidb::UserDefinedFunction asciify_str (scidb::FunctionDescription("codify", list_of("string"), "string", &codify ));
