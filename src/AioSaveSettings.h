/*
 **
 * BEGIN_COPYRIGHT
 *
 * PARADIGM4 INC.
 * This file is part of the Paradigm4 Enterprise SciDB distribution kit
 * and may only be used with a valid Paradigm4 contract and in accord
 * with the terms and conditions specified by that contract.
 *
 * Copyright (C) 2010 - 2015 Paradigm4 Inc.
 * All Rights Reserved.
 *
 * END_COPYRIGHT
 */

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/Operator.h>
#include <log4cxx/logger.h>

#ifndef UNPARSE_SETTINGS
#define UNPARSE_SETTINGS

#ifdef CPP11
using std::shared_ptr;
#else
using boost::shared_ptr;
#endif

using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;
using namespace std;

namespace scidb
{

class AioSaveSettings
{
private:
	int64_t      _cellsPerChunk;
	bool         _cellsPerChunkSet;
    char         _attributeDelimiter;
    bool         _attributeDelimiterSet;
    char         _lineDelimiter;
    bool         _lineDelimiterSet;
    InstanceID   _saveInstanceId;
    size_t const _numInstances;
    bool         _instanceSet;
    string       _filePath;
    bool         _filePathSet;
    bool         _binaryFormat;
    string       _binaryFormatString;
    bool         _formatSet;

public:
    static const size_t MAX_PARAMETERS = 6;

    AioSaveSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                    bool logical,
                    shared_ptr<Query>& query):
				_cellsPerChunk(1000000),
				_cellsPerChunkSet(false),
                _attributeDelimiter('\t'),
                _attributeDelimiterSet(false),
                _lineDelimiter('\n'),
                _lineDelimiterSet(false),
                _saveInstanceId(0),
                _numInstances(query->getInstancesCount()),
                _instanceSet(false),
                _filePath(""),
                _filePathSet(false),
                _binaryFormat(false),
                _binaryFormatString(""),
                _formatSet(false)
    {
    	string const cellsPerChunkHeader           = "cells_per_chunk=";
    	string const attributeDelimiterHeader      = "attribute_delimiter=";
    	string const lineDelimiterHeader           = "line_delimiter=";
    	string const filePathHeader                = "path=";
    	string const formatHeader                  = "format=";
    	string const instanceHeader                = "instance=";
    	size_t const nParams = operatorParameters.size();
    	if (nParams > MAX_PARAMETERS)
    	{   //assert-like exception. Caller should have taken care of this!
    		throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to UnparseSettings";
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
    		if (starts_with(parameterString, cellsPerChunkHeader))
    		{
    			if (_cellsPerChunkSet)
    			{
    				throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set cells_per_chunk multiple times";
    			}
    			string paramContent = parameterString.substr(cellsPerChunkHeader.size());
    			trim(paramContent);
    			try
    			{
    				_cellsPerChunk = lexical_cast<int64_t>(paramContent);
    				if(_cellsPerChunk<=0)
    				{
    					throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "cells_per_chunk must be positive";
    				}
    			}
    			catch (bad_lexical_cast const& exn)
    			{
    				throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse cells_per_chunk";
    			}
    		}
    		else if (starts_with(parameterString, attributeDelimiterHeader))
    		{
    		    if (_attributeDelimiterSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set attribute_delimiter multiple times";
                }
                string paramContent = parameterString.substr(attributeDelimiterHeader.size());
                trim(paramContent);
                if (paramContent == "\\t")
                {
                    _attributeDelimiter = '\t';
                }
                else if (paramContent == "\\r")
                {
                    _attributeDelimiter = '\r';
                }
                else if (paramContent == "\\n")
                {
                    _attributeDelimiter = '\n';
                }
                else if (paramContent == "")
                {
                    _attributeDelimiter = ' ';
                }
                else
                {
                    try
                    {
                        _attributeDelimiter = lexical_cast<char>(paramContent);
                    }
                    catch (bad_lexical_cast const& exn)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse attribute_delimiter";
                    }
                }
                _attributeDelimiterSet = true;
            }
            else if (starts_with (parameterString, lineDelimiterHeader))
            {
                if(_lineDelimiterSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set line_delimiter multiple times";
                }
                string paramContent = parameterString.substr(lineDelimiterHeader.size());
                trim(paramContent);
                if (paramContent == "\\t")
                {
                    _lineDelimiter = '\t';
                }
                else if (paramContent == "\\r")
                {
                    _lineDelimiter = '\r';
                }
                else if (paramContent == "\\n")
                {
                    _lineDelimiter = '\n';
                }
                else if (paramContent == "")
                {
                    _lineDelimiter = ' ';
                }
                else
                {
                    try
                    {
                        _lineDelimiter = lexical_cast<char>(paramContent);
                    }
                    catch (bad_lexical_cast const& exn)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse line_delimiter";
                    }
                }
                _lineDelimiterSet = true;
            }
            else if (starts_with (parameterString, filePathHeader))
            {
                if(_lineDelimiterSet)
                {
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set file_path multiple times";
                }
                string paramContent = parameterString.substr(filePathHeader.size());
                trim(paramContent);
                _filePath = paramContent;
                _filePathSet = true;
            }
            else if (starts_with (parameterString, formatHeader))
            {
                if(_formatSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set format multiple times";
                }
                string paramContent = parameterString.substr(formatHeader.size());
                trim(paramContent);
                if(paramContent == "tdv" || paramContent == "tsv")
                {
                    _binaryFormat = false;
                }
                else
                {
                    _binaryFormat = true;
                    if(paramContent[0]!='(' || paramContent[paramContent.size()-1] != ')')
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "format must be either 'tdv' or a binary spec such as '(int64,double,string null)'";
                    }
                    _binaryFormatString = paramContent;
                }
                _formatSet=true;
            }
            else if (starts_with (parameterString, instanceHeader))
            {
                if(_instanceSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set instance multiple times";
                }
                string paramContent = parameterString.substr(instanceHeader.size());
                trim(paramContent);
                try
                {
                    int64_t iid = lexical_cast<int64_t>(paramContent);
                    if(iid<0 || ((uint64_t)iid) >= _numInstances)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "header must be positive";
                    }
                    _saveInstanceId = iid;
                    _instanceSet = true;
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse header";
                }
            }
            else
    		{
                string path = parameterString;
                trim(path);
                bool containsStrangeCharacters = false;
                for(size_t i=0; i<path.size(); ++i)
                {
                   if(path[i] == '=' || path[i] == ' ')
                   {
                       containsStrangeCharacters=true;
                       break;
                   }
                }
                if (_filePathSet || containsStrangeCharacters)
                {
                  ostringstream errorMsg;
                  errorMsg << "unrecognized parameter: "<< parameterString;
                  throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << errorMsg.str().c_str();
                }
                _filePath = path;
                _filePathSet = true;
    		}
    	}
    	if(!_filePathSet)
    	{
    	    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "file_path must be provided";
    	}
    	if(_binaryFormat && (_lineDelimiterSet || _attributeDelimiterSet))
    	{
    	    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "attribute_delimiter and line_delimiter are not used with the binary fomat";
    	}
    }

    size_t getLinesPerChunk() const
    {
        return _cellsPerChunk;
    }

    char getAttributeDelimiter() const
    {
    	return _attributeDelimiter;
    }

    char getLineDelimiter() const
    {
    	return _lineDelimiter;
    }

    InstanceID getSaveInstanceId() const
    {
        return _saveInstanceId;
    }

    string const& getFilePath() const
    {
        return _filePath;
    }

    bool isBinaryFormat() const
    {
        return _binaryFormat;
    }

    string const& getBinaryFormatString() const
    {
        return _binaryFormatString;
    }
};

}


#endif //UnparseSettings
