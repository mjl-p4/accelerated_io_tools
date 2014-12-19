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
#include <boost/algorithm/string.hpp>

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

static scidb::UserDefinedFunction dcast_double (scidb::FunctionDescription("dcast", list_of("string")("double"), "double", &dcast<double> ));
static scidb::UserDefinedFunction dcast_float (scidb::FunctionDescription("dcast", list_of("string")("float"), "float", &dcast<float> ));
static scidb::UserDefinedFunction dcast_bool (scidb::FunctionDescription("dcast", list_of("string")("bool"), "bool", &dcast<bool> ));
static scidb::UserDefinedFunction dcast_int64  (scidb::FunctionDescription("dcast", list_of("string")("int64"),  "int64" , &dcast<int64_t>));
static scidb::UserDefinedFunction dcast_int32 (scidb::FunctionDescription("dcast", list_of("string")("int32"), "int32", &dcast<int32_t> ));
static scidb::UserDefinedFunction dcast_int16 (scidb::FunctionDescription("dcast", list_of("string")("int16"), "int16", &dcast<int16_t> ));
static scidb::UserDefinedFunction dcast_int8 (scidb::FunctionDescription("dcast", list_of("string")("int8"), "int8", &dcast<int8_t> ));
static scidb::UserDefinedFunction dcast_uint64 (scidb::FunctionDescription("dcast", list_of("string")("uint64"), "uint64", &dcast<uint64_t> ));
static scidb::UserDefinedFunction dcast_uint32 (scidb::FunctionDescription("dcast", list_of("string")("uint32"), "uint32", &dcast<uint32_t> ));
static scidb::UserDefinedFunction dcast_uint16 (scidb::FunctionDescription("dcast", list_of("string")("uint16"), "uint16", &dcast<uint16_t> ));
static scidb::UserDefinedFunction dcast_uint8 (scidb::FunctionDescription("dcast", list_of("string")("uint8"), "uint8", &dcast<uint8_t> ));

// XXX How to add datetime conversion here? The naive approach:
//static scidb::UserDefinedFunction dcast_datetimetz (scidb::FunctionDescription("dcast", list_of("string")("datetimetz"), "datetimetz", &dcast<datetimetz> ));
// doesn't work!

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

//in some rare cases, on older versions, string values are not null-terminated; we aim to be nice
string get_null_terminated_string(char const* input, size_t const size)
{
    if(size == 0)
    {
        return string("");
    }
    else if (input[size-1] != 0)
    {
        return string(input, size);
    }
    else
    {
        return string(input);
    }
}


static void keyed_value( const Value** args, Value *res, void* )
{
    if(args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    else if (args[1]->isNull())
    {
        res->setNull(1);
        return;
    }
    string cell =        get_null_terminated_string(args[0]->getString(), args[0]->size());
    string info_field =  get_null_terminated_string(args[1]->getString(), args[1]->size());
    vector<string> values;
    split(values, cell, is_from_range(';', ';'));
    for(size_t i=0, n=values.size(); i<n; ++i)
    {
        vector<string> pair;
        split(pair, values[i], is_from_range('=','='));
        if(pair.size()!=2)
        {
            res->setNull(2);
            return;
        }
        if(pair[0]==info_field)
        {
            res->setString(pair[1]);
            return;
        }
    }
    res->setNull(0);
}
static scidb::UserDefinedFunction key_value_extract( scidb::FunctionDescription("keyed_value", list_of("string")("string")("string"), "string", &keyed_value));



/**
 * Loosely based on https://github.com/slottad/scidb-genotypes/
 * Thanks to Douglas Slotta.
 * We find we are repeating some of the work and it might make sense to merge in more of that functionality
 */
void num_csv(const scidb::Value** args, scidb::Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    string cell = get_null_terminated_string(args[0]->getString(), args[0]->size());
    uint32_t count = 0;
    if (!cell.empty())
    {
        count = 1 + std::count(cell.begin(), cell.end(), ',');
    }
    res->setUint32(count);
}
static scidb::UserDefinedFunction ncsv( scidb::FunctionDescription("num_csv", list_of("string"), "uint32", &num_csv));

void nth_csv(const scidb::Value** args, scidb::Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    if (args[1]->isNull())
    {
        res->setNull(0);
        return;
    }
    uint32_t n = args[1]->getUint32();
    string cell = get_null_terminated_string(args[0]->getString(), args[0]->size());
    vector<string> values;
    split(values, cell, is_from_range(',', ','));
    if (values.size() <= n)
    {
        res->setNull(0);
        return;
    }
    res->setString(values[n]);
}

static scidb::UserDefinedFunction ntcsv( scidb::FunctionDescription("nth_csv", list_of("string")("uint32"), "string", &nth_csv));

void maxlen_csv(const scidb::Value** args, scidb::Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    string cell = get_null_terminated_string(args[0]->getString(), args[0]->size());
    vector<string> values;
    split(values, cell, is_from_range(',', ','));
    uint32_t maxSize =0;
    for(size_t i=0, n=values.size(); i<n; ++i)
    {
        if(values[i].size()> maxSize)
            maxSize=values[i].size();
    }
    res->setUint32(maxSize);
}

static scidb::UserDefinedFunction mlcsv( scidb::FunctionDescription("maxlen_csv", list_of("string"), "uint32", &maxlen_csv));


/**
 * arg0: FORMAT FIELD
 * arg1: sample FIELD
 * arg2: attribute name
 */
static void extract_format_field( const Value **args, Value* res, void*) {
    for(int i = 0; i < 3; i++) { 
        if(args[i]->isNull()) { 
            res->setNull(args[i]->getMissingReason());
            return;
        }
    }

    const char* formatField = args[0]->getString();
    size_t formatLen = args[0]->size();
    const char* sampleField = args[1]->getString();
    size_t sampleLen = args[1]->size();
    const char* attrName = args[2]->getString();
    size_t attrLen = args[2]->size();

    size_t index = 0;
    size_t j = 0, k = 0;
    bool match = false;
    for(; j < formatLen && !match; j += k) { 
      if(formatField[j] == ':') { index++; j++; }
      match = true;
      for(k = 0; j+k < formatLen && formatField[j+k] != ':'; k++) { 
        if(k >= attrLen || formatField[j+k] != attrName[k]) { match = false; }
      }
    }

    if(!match) { 
      res->setNull(0);
      return;
    }

    size_t start = 0;
    size_t indexi = 0;
    for(; start < sampleLen && indexi < index; start++) { 
        if(sampleField[start] == ':') { 
            indexi += 1;
        }
    }
    
    size_t end = start+1;
    for(; end < sampleLen && sampleField[end] != ':'; end += 1) 
        {}

    size_t size = end - start + 1;
    res->setSize(size);
    memcpy(res->data(), &sampleField[start], (end-start));
    ((char*)res->data())[size-1]=0;
}

static scidb::UserDefinedFunction format_extract(scidb::FunctionDescription("format_extract", list_of("string")("string")("string"), "string", &extract_format_field));
