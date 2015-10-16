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


#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <query/Operator.h>

#ifndef UBER_LOAD_SETTINGS
#define UBER_LOAD_SETTINGS

#ifdef CPP11
using std::shared_ptr;
#else
using boost::shared_ptr;
#endif

using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;
using boost::algorithm::trim;
using namespace boost::filesystem;
using namespace std;

namespace scidb
{
class UberLoadSettings
{
private:

    bool             _singlepath;
    bool             _multiplepath;
    string           _inputFilePath;
    vector<string>   _inputPaths;
    vector<int64_t>  _inputInstances;
    int64_t          _instanceParse;
    int64_t          _bufferSize;
    bool             _bufferSizeSet;
    int64_t          _header;
    bool             _headerSet;
    char             _lineDelimiter;
    bool             _lineDelimiterSet;
    char             _attributeDelimiter;
    bool             _attributeDelimiterSet;
    int64_t          _numAttributes;
    int64_t          _chunkSize;
    bool             _chunkSizeSet;
    bool             _splitOnDimension;
    bool             _splitOnDimensionSet;

public:
    static const size_t MAX_PARAMETERS = 9;

    UberLoadSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                     bool logical,
                     shared_ptr<Query>& query):
       _singlepath(false),
       _multiplepath(false),
       _inputFilePath(""),
       _instanceParse(-1),
       _bufferSize(8*1024*1024),
       _bufferSizeSet(false),
       _header(0),
       _headerSet(false),
       _lineDelimiter('\n'),
       _lineDelimiterSet(false),
       _attributeDelimiter('\t'),
       _attributeDelimiterSet(false),
       _numAttributes(0),
       _chunkSize(10000000),
       _chunkSizeSet(false),
       _splitOnDimension(false),
       _splitOnDimensionSet(false)
    {
        string const inputFilePathHeader        = "path=";
        string const inputPathsHeader           = "paths=";
        string const inputInstancesHeader       = "instances=";
        string const bufferSizeHeader           = "buffer_size=";
        string const headerHeader               = "header=";
        string const lineDelimiterHeader        = "line_delimiter=";
        string const attributeDelimiterHeader   = "attribute_delimiter=";
        string const numAttributesHeader        = "num_attributes=";
        string const chunkSizeHeader            = "chunk_size=";
        string const splitOnDimensionHeader     = "split_on_dimension=";

        int64_t const myInstanceId = query->getInstanceID();
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
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the path multiple times";
                }
                string paramContent = parameterString.substr(inputFilePathHeader.size());
                trim(paramContent);
                _singlepath = true;
                _inputFilePath = paramContent;
                _instanceParse = 0;
            }
            else if  (starts_with(parameterString, inputPathsHeader))
            {
                if (_inputPaths.size() > 0)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the paths multiple times";
                }
                string paramContent = parameterString.substr(inputPathsHeader.size());
                trim(paramContent);
                char delimiter=';';
                vector<string> internal;
                stringstream ss(paramContent); // Turn the string into a stream.
                string tok;
                _multiplepath = true;
                while(getline(ss, tok, delimiter)) 
                {
                    _inputPaths.push_back(tok);
                }
            }
            else if  (starts_with(parameterString, inputInstancesHeader))
            {
                if (_inputInstances.size() > 0)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the input instances multiple times";
                }
                string paramContent = parameterString.substr(inputInstancesHeader.size());
                trim(paramContent);
                char delimiter=';';
                vector<string> internal;
                stringstream ss(paramContent); // Turn the string into a stream.
                string tok;
                while(getline(ss, tok, delimiter)) 
                {
                    _inputInstances.push_back(lexical_cast<int64_t>(tok));
                }
            }
            else if (starts_with(parameterString, headerHeader))
            {
                if (_headerSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the header multiple times";
                }
                string paramContent = parameterString.substr(headerHeader.size());
                trim(paramContent);
                try
                {
                    _header = lexical_cast<int64_t>(paramContent);
                    if(_header<=0)
                   {
                       throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "header must be positive";
                   }
                    _headerSet = true;
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse header";
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
                    if(_bufferSize<=8)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "buffer_size must be greater than 8";
                    }
                    if(_bufferSize>= 1024*1024*1024)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "buffer_size must be under 1GB";
                    }
                    _bufferSizeSet = true;
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse buffer_size";
                }
            }
            else if (starts_with(parameterString, lineDelimiterHeader))
            {
                if(_lineDelimiterSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set line delimiter multiple times";
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
                       throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse delimiter";
                    }
                }
                _lineDelimiterSet = true;
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
            else if  (starts_with(parameterString, numAttributesHeader))
            {
                if (_numAttributes != 0)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set the number of attributes multiple times";
                }
                string paramContent = parameterString.substr(numAttributesHeader.size());
                trim(paramContent);
                try
                {
                    _numAttributes = lexical_cast<int64_t>(paramContent);
                    if(_numAttributes<=0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "num_attributes must be positive";
                    }
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse num_attributes";
                }
            }
            else if (starts_with (parameterString, splitOnDimensionHeader))
            {
                if(_splitOnDimensionSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set split_on_dimension multiple times";
                }
                string paramContent = parameterString.substr(splitOnDimensionHeader.size());
                trim(paramContent);
                try
                {
                   _splitOnDimension = lexical_cast<bool>(paramContent);
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse split_on_dimension";
                }
            }
            else if (starts_with(parameterString, chunkSizeHeader))
            {
                if (_chunkSizeSet)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set chunk_size multiple times";
                }
                string paramContent = parameterString.substr(chunkSizeHeader.size());
                trim(paramContent);
                try
                {
                    _chunkSize = lexical_cast<int64_t>(paramContent);
                    if(_chunkSize<=0)
                    {
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk_size must be positive";
                    }
                    _chunkSizeSet = true;
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse chunk_size";
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
                if (_inputFilePath != "" || containsStrangeCharacters)
                {
                   ostringstream errorMsg;
                   errorMsg << "unrecognized parameter: "<< parameterString;
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << errorMsg.str().c_str();
                }
                _singlepath     = true;
                _inputFilePath  = path;
                _instanceParse  = 0;
            }
        }
        if(_multiplepath)
        {
            if(_inputInstances.size() != _inputPaths.size())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Number of paths do not equal the number of instances.";
            }
            std::set<string>uniqueInstances;
            std::set<int64_t> s(_inputInstances.begin(), _inputInstances.end()); 
            if(s.size() !=  _inputPaths.size())
            {
               throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Input instances were not unique.";
            }
            if (_singlepath == true)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Both single path and multiple path were declared.";
            }
            std::vector<int64_t>::iterator itrel = std::find(_inputInstances.begin(), _inputInstances.end(), -1);
            if((_inputInstances.size()==1) && (itrel != _inputInstances.end()))
            {
                 string relinputpath  = _inputPaths[0];
                 _inputFilePath  = relinputpath;  
                 _instanceParse  = myInstanceId;
            }
            else
            {
                int const numinstances = query->getInstancesCount()-1;
                if (std::find_if (_inputInstances.begin(), _inputInstances.end() , bind2nd(greater<int64_t>(),numinstances)) != _inputInstances.end())
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "instance specified that is greater than numinstances";
                }
                if (std::find_if (_inputInstances.begin(), _inputInstances.end() , bind2nd(less<int64_t>(),0)) != _inputInstances.end())
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "instance specified that is less than 0";
                }
                std::vector<int64_t>::iterator it = std::find(_inputInstances.begin(), _inputInstances.end(), myInstanceId);
                if (it != _inputInstances.end())
                {
                    int64_t index = std::distance(_inputInstances.begin(), it);
                    _instanceParse       = myInstanceId;
                    _inputFilePath       = _inputPaths[index];
                }
            }
        }
        else if (_inputInstances.size() > 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "The instances argument is not used with a single file path";
        }
        else if (_inputFilePath.size() == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "No input file path was provided";
        }
        if (_numAttributes == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "num_attributes was not provided";
        }
        if (_bufferSizeSet && !_chunkSizeSet)
        {
            _chunkSize = _bufferSize;
        }
    }

    int64_t const& getParseInstance() const
    {
        return _instanceParse;
    }

    string const& getInputFilePath() const
    {
        return _inputFilePath;
    }

    size_t getBlockSize() const
    {
        return _bufferSize;
    }

    char getLineDelimiter() const
    {
        return _lineDelimiter;
    }

    char getAttributeDelimiter() const
    {
        return _attributeDelimiter;
    }

    int64_t getHeader() const
    {
        return _header;
    }

    int64_t getChunkSize() const
    {
        return _chunkSize;
    }

    int64_t getNumAttributes() const
    {
        return _numAttributes;
    }

    bool getSplitOnDimension() const
    {
        return _splitOnDimension;
    }
};

}

#endif //UBER_LOAD_SETTINGS
