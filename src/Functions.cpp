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

/**
 * DCAST: cast with default, does not throw an error. Tries to cast input to the appropriate type. If the cast fails,
 * returns the supplied default.
 */
template <typename T, bool eight_bit>
static void dcast(const Value** args, Value *res, void*)
{
  if(args[0]->isNull())
  {
      res->setNull(args[0]->getMissingReason());
      return;
  }
  string s = get_null_terminated_string(args[0]->getString(), args[0]->size());
  try
  {
      T result;
      if (!eight_bit)
      {
          result = lexical_cast<T>(s);
      }
      else //there's an extra coockalacka here because lexical_cast<uint8_t> resolves to lexical_cast<char>
           //which turns an input like '6' to the corresponding ASCII character value (54). This is a workaround
           //directly from the boost web page
      {
          result = numeric_cast <T> (lexical_cast<int32_t> (s));
      }
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

static scidb::UserDefinedFunction dcast_double (scidb::FunctionDescription("dcast", list_of("string")("double"),  "double", &dcast<double,   false> ));
static scidb::UserDefinedFunction dcast_float  (scidb::FunctionDescription("dcast", list_of("string")("float"),   "float",  &dcast<float,    false> ));
static scidb::UserDefinedFunction dcast_bool   (scidb::FunctionDescription("dcast", list_of("string")("bool"),    "bool",   &dcast<bool,     false> ));
static scidb::UserDefinedFunction dcast_int64  (scidb::FunctionDescription("dcast", list_of("string")("int64"),   "int64" , &dcast<int64_t,  false> ));
static scidb::UserDefinedFunction dcast_int32  (scidb::FunctionDescription("dcast", list_of("string")("int32"),   "int32",  &dcast<int32_t,  false> ));
static scidb::UserDefinedFunction dcast_int16  (scidb::FunctionDescription("dcast", list_of("string")("int16"),   "int16",  &dcast<int16_t,  false> ));
static scidb::UserDefinedFunction dcast_uint64 (scidb::FunctionDescription("dcast", list_of("string")("uint64"),  "uint64", &dcast<uint64_t, false> ));
static scidb::UserDefinedFunction dcast_uint32 (scidb::FunctionDescription("dcast", list_of("string")("uint32"),  "uint32", &dcast<uint32_t, false> ));
static scidb::UserDefinedFunction dcast_uint16 (scidb::FunctionDescription("dcast", list_of("string")("uint16"),  "uint16", &dcast<uint16_t, false> ));
static scidb::UserDefinedFunction dcast_uint8  (scidb::FunctionDescription("dcast", list_of("string")("uint8"),   "uint8",  &dcast<uint8_t,  true>  ));
static scidb::UserDefinedFunction dcast_int8   (scidb::FunctionDescription("dcast", list_of("string")("int8"),    "int8",   &dcast<int8_t,   true>  ));

// XXX To add a datetime conversion here, need to find a boost routine that does it, and/or replicate what parseDateTime (TypeSystem.cpp) does

template <bool trim_characters_supplied>
static void trim (const Value** args, Value *res, void*)
{
    if(args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    string characters = " ";
    if(trim_characters_supplied)
    {
        if(args[1]->isNull())
        {
            res->setNull(0);
            return;
        }
        characters = get_null_terminated_string(args[1]->getString(), args[1]->size());
    }
    string input = get_null_terminated_string(args[0]->getString(), args[0]->size());
    trim_if(input, is_any_of(characters));
    res->setString(input);
}

static scidb::UserDefinedFunction trim_space (scidb::FunctionDescription("trim", list_of("string"),           "string", &trim<false> ));
static scidb::UserDefinedFunction trim_str   (scidb::FunctionDescription("trim", list_of("string")("string"), "string", &trim<true> ));

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
    (*res) = (*args[2]);
}
static scidb::UserDefinedFunction key_value_extract( scidb::FunctionDescription("keyed_value", list_of("string")("string")("string"), "string", &keyed_value));

void char_count(const scidb::Value** args, scidb::Value* res, void*)
{
    if(args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    string input =      get_null_terminated_string(args[0]->getString(), args[0]->size());
    if(args[1]->isNull())
    {
        res->setNull(0);
        return;
    }
    string separator =  get_null_terminated_string(args[1]->getString(), args[1]->size());
    if (separator.size() == 0)
    {
        res->setNull(1);
        return;
    }
    size_t sepSize = separator.size();
    uint32_t count = 0;
    for (size_t i =0, s=input.size(); i<s; ++i) //XXX: is there a boost any_of splitter for this? One that does not require splitting?
    {
        for(size_t j=0; j<sepSize; ++j)
        {
            if(input[i] == separator[j])
            {
                ++count;
                break;
            }
        }
    }
    res -> setUint32(count);
}
static scidb::UserDefinedFunction ntdv( scidb::FunctionDescription("char_count", list_of("string")("string"), "uint32", &char_count));

template <bool custom_separator>
void nth_tdv(const scidb::Value** args, scidb::Value* res, void*)
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
    string separator = ",";
    if(custom_separator)
    {
       if(args[2]->isNull())
       {
           res->setNull(0);
           return;
       }
       separator = get_null_terminated_string(args[2]->getString(), args[2]->size());
       if(separator.size()==0)
       {
           res->setNull(0);
           return;
       }
    }
    uint32_t n = args[1]->getUint32();
    string cell = get_null_terminated_string(args[0]->getString(), args[0]->size());
    vector<string> values;
    split(values, cell, is_any_of(separator));
    if (values.size() <= n)
    {
        res->setNull(0);
        return;
    }
    res->setString(values[n]);
}
static scidb::UserDefinedFunction ntcsv( scidb::FunctionDescription("nth_csv", list_of("string")("uint32"),           "string", &nth_tdv<false>));
static scidb::UserDefinedFunction nttdv( scidb::FunctionDescription("nth_tdv", list_of("string")("uint32")("string"), "string", &nth_tdv<true>));

template <bool custom_separator>
void maxlen_tdv(const scidb::Value** args, scidb::Value* res, void*)
{
    if (args[0]->isNull())
    {
        res->setNull(args[0]->getMissingReason());
        return;
    }
    string cell = get_null_terminated_string(args[0]->getString(), args[0]->size());
    vector<string> values;
    string separator = ",";
    if(custom_separator)
    {
       if(args[1]->isNull())
       {
           res->setNull(0);
           return;
       }
       separator = get_null_terminated_string(args[1]->getString(), args[1]->size());
       if(separator.size()==0)
       {
           res->setNull(0);
           return;
       }
    }
    split(values, cell, is_any_of(separator));
    uint32_t maxSize =0;
    for(size_t i=0, n=values.size(); i<n; ++i)
    {
        if(values[i].size()> maxSize)
            maxSize=values[i].size();
    }
    res->setUint32(maxSize);
}

static scidb::UserDefinedFunction mlcsv( scidb::FunctionDescription("maxlen_csv", list_of("string"),           "uint32", &maxlen_tdv<false>));
static scidb::UserDefinedFunction mltdv( scidb::FunctionDescription("maxlen_tdv", list_of("string")("string"), "uint32", &maxlen_tdv<true>));

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

void toss(const scidb::Value** args, scidb::Value* res, void*)
{
    string error;
    if(args[0]->isNull())
    {
        error = "null";
    }
    else
    {
        error = get_null_terminated_string(args[0]->getString(), args[0]->size());
    }
    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error;
}
static scidb::UserDefinedFunction errortoss( scidb::FunctionDescription("throw", list_of("string"), "uint8", &toss));
