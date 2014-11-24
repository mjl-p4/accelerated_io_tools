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

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>

#ifndef SPLIT_SETTINGS
#define SPLIT_SETTINGS

namespace scidb
{

class SplitSettings
{
private:
    string  _inputFilePath;
    int64_t _linesPerChunk;
    bool    _linesPerChunkSet;
    int64_t _bufferSize;
    bool    _bufferSizeSet;
    int64_t _sourceInstanceId;
    bool    _sourceInstanceIdSet;
    char    _delimiter;
    bool    _delimiterSet;

public:
    static const size_t MAX_PARAMETERS = 5;

    SplitSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                 bool logical,
                 shared_ptr<Query>& query):
       _inputFilePath(""),
       _linesPerChunk(1000000),
       _linesPerChunkSet(false),
       _bufferSize(10*1024*1024),
       _bufferSizeSet(false),
       _sourceInstanceId(0),
       _sourceInstanceIdSet(false),
       _delimiter('\n'),
       _delimiterSet(false)
    {
        string const inputFilePathHeader        = "input_file_path=";
        string const linesPerChunkHeader        = "lines_per_chunk=";
        string const bufferSizeHeader           = "buffer_size=";
        string const sourceInstanceIdHeader     = "source_instance_id=";
        string const delimiterHeader            = "delimiter=";
        size_t const nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to SplitSettings";
        }
        for (size_t i= 0; i<nParams; ++i)
        {
            shared_ptr<OperatorParam>const& param = operatorParameters[i];
            string parameterString;
            if (logical)
            {
                parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(),query, TID_STRING).getString();
            }
            else
            {
                parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
            }
            if      (starts_with(parameterString, inputFilePathHeader))
            {
                if (_inputFilePath != "")
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the input file path multiple times";
                }
                string paramContent = parameterString.substr(inputFilePathHeader.size());
                trim(paramContent);
                _inputFilePath = paramContent;
            }
            else if (starts_with(parameterString, linesPerChunkHeader))  //yeah, yeah it's a long function...
            {
                if (_linesPerChunkSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set lines_per_chunk multiple times";
                }
                string paramContent = parameterString.substr(linesPerChunkHeader.size());
                trim(paramContent);
                try
                {
                    _linesPerChunk = lexical_cast<int64_t>(paramContent);
                    if(_linesPerChunk<=0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "lines_per_chunk must be positive";
                    }
                    _linesPerChunkSet = true;
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse lines_per_chunk";
                }
            }
            else if (starts_with(parameterString, bufferSizeHeader))
            {
                if (_bufferSizeSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set buffer_size multiple times";
                }
                string paramContent = parameterString.substr(bufferSizeHeader.size());
                trim(paramContent);
                try
                {
                    _bufferSize = lexical_cast<int64_t>(paramContent);
                    if(_bufferSize<=0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "lines_per_chunk must be positive";
                    }
                    _bufferSizeSet = true;
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse buffer_size";
                }
            }
            else if (starts_with (parameterString, sourceInstanceIdHeader))
            {
                if(_sourceInstanceIdSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set source_instance_id multiple times";
                }
                string paramContent = parameterString.substr(sourceInstanceIdHeader.size());
                trim(paramContent);
                try
                {
                    _sourceInstanceId = lexical_cast<int64_t>(paramContent);
                    if(_sourceInstanceId != 0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "source_instance_id may only be 0 (for now)";
                    }
                    _sourceInstanceIdSet = true;
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse source_instance_id";
                }
            }
            else if (starts_with(parameterString, delimiterHeader))
            {
                if(_delimiterSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set delimiter multiple times";
                }
                string paramContent = parameterString.substr(delimiterHeader.size());
                trim(paramContent);
                if (paramContent == "\\t")
                {
                    _delimiter = '\t';
                }
                else if (paramContent == "\\r")
                {
                    _delimiter = '\r';
                }
                else if (paramContent == "\\n")
                {
                    _delimiter = '\n';
                }
                else if (paramContent == "")
                {
                    _delimiter = ' ';
                }
                else
                {
                    try
                    {
                       _delimiter = lexical_cast<char>(paramContent);
                    }
                    catch (bad_lexical_cast const& exn)
                    {
                       throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse delimiter";
                    }
                }
                _delimiterSet = true;
            }
            else
            {
                if (_inputFilePath != "")
                {
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the input file path multiple times";
                }
                string path = parameterString;
                trim(path);
                _inputFilePath = path;
            }
        }
        if (_inputFilePath == "")
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "input file path was not provided";
        }
    }

    string const& getInputFilePath() const
    {
        return _inputFilePath;
    }

    size_t getLinesPerChunk() const
    {
        return _linesPerChunk;
    }

    size_t getBufferSize() const
    {
        return _bufferSize;
    }

    int64_t getSourceInstanceId() const
    {
        return _sourceInstanceId;
    }

    char getDelimiter() const
    {
        return _delimiter;
    }
};

}

#endif //SPLIT_SETTINGS
