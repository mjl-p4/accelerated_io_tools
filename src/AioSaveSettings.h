/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2020 Paradigm4 Inc.
* All Rights Reserved.
*
* accelerated_io_tools is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* accelerated_io_tools is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* accelerated_io_tools is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with accelerated_io_tools.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <query/Expression.h>
#include <util/PathUtils.h>

#ifndef AIO_SAVE_SETTINGS
#define AIO_SAVE_SETTINGS

using boost::algorithm::trim;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;
using namespace std;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.aio_save"));

namespace scidb
{

static const char* const KW_PATHS 			= "paths";
static const char* const KW_INSTANCES		= "instances";
static const char* const KW_BUF_SZ 			= "buffer_size";
static const char* const KW_LINE_DELIM		= "line_delimiter";
static const char* const KW_ATTR_DELIM		= "attribute_delimiter";
static const char* const KW_CELLS_PER_CHUNK	= "cells_per_chunk";
static const char* const KW_FORMAT			= "format";
static const char* const KW_NULL_PATTERN	= "null_pattern";
static const char* const KW_PRECISION		= "precision";
static const char* const KW_ATTS_ONLY		= "atts_only";
static const char* const KW_RESULT_LIMIT	= "result_size_limit";

typedef std::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;

class AioSaveSettings
{
public:
    static size_t chunkDataOffset()
    {
        return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 5);
    }

    static size_t chunkSizeOffset()
    {
        return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 1);
    }


private:
    enum FormatType
    {
        TEXT   = 0,
        BINARY = 1,
        ARROW  = 2
    };

    size_t                      _bufferSize;
    int64_t                     _cellsPerChunk;
    char                        _attributeDelimiter;
    char                        _lineDelimiter;
    map<InstanceID, string>     _instancesAndPaths;
    size_t const                _numInstances;
    FormatType                  _format;
    string                      _binaryFormatString;
    string                      _nullPrefix;
    bool                        _printNullCode;
    string                      _nullPostfix;
    bool                        _printCoordinates;
    bool                        _quoteStrings;
    bool                        _writeHeader;
    int32_t                     _precision;
    bool                        _attsOnly;
    int64_t                     _resultSizeLimit;
    bool  						_usingCsvPlus;
    vector<string>			    _filePaths;
    vector<InstanceID>			_instanceIds;

    void checkIfSet(bool alreadySet, const char* kw)
    {
        if (alreadySet)
        {
            ostringstream error;
            error<<"illegal attempt to set "<<kw<<" multiple times";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << error.str().c_str();
        }
    }

    void setParamPaths(vector<string> paths)
    {
        if (_filePaths.size() > 0) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set file paths multiple times";
        }

        for (size_t i = 0; i < paths.size(); ++i)
        {
            _filePaths.push_back(paths[i]);
        }
    }

    void setParamCellsPerChunk(vector<int64_t> cells_per_chunk)
    {
        _cellsPerChunk = cells_per_chunk[0];
        if(_cellsPerChunk <= 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "cells_per_chunk must be positive";
        }
    }

    void setParamInstances(vector<int64_t> instances)
    {
        if (_instanceIds.size() > 0) {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set instances multiple times";
        }

        for (size_t i = 0; i < instances.size(); ++i) {
           _instanceIds.push_back(instances[i]);
        }
    }

    void setParamFormat(vector<string> format)
    {
        if(format[0] == "tdv" || format[0] == "tsv" || format[0] == "csv+" || format[0] == "lcsv+")
        {
            _format = TEXT;
            if(format[0] == "csv+" || format[0] == "lcsv+")
            {
                _usingCsvPlus = true;
            }
        }
        else if(format[0] == "arrow")
        {
            _format = ARROW;
        }
        else
        {
            _format = BINARY;
            LOG4CXX_DEBUG(logger, "aio_save binary format first char: " << format[0][0]);
            LOG4CXX_DEBUG(logger, "aio_save binary format last char: " << format[0][format[0].size()-1]);
            if(format[0][0]!='(' || format[0][format[0].size()-1] != ')')
            {
                LOG4CXX_DEBUG(logger, "aio_save binary format is: " << format[0]);
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "format must be either 'tdv', 'tsv', 'csv+', 'arrow', or a binary spec such as '(int64,double,string null)'";
            }
            _binaryFormatString = format[0];
        }
    }

    void setParamBufferSize(vector<int64_t> buf_size)
    {
        _bufferSize = buf_size[0];
        size_t const bufferSizeLimit = chunkDataOffset() + 8;
        if(_bufferSize <= bufferSizeLimit)
        {
            ostringstream err;
            err << "buffer_size must be above " << bufferSizeLimit;
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str();
        }
    }

    void setParamResultSizeLimit(vector<int64_t> result_size_lim)
    {
        _resultSizeLimit = result_size_lim[0];
        if(_resultSizeLimit < 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "tmp_sz_limit must be positive";
        }
    }

    void setParamLineDelim(vector<string> l_delim)
    {
        _lineDelimiter = getParamDelim(l_delim);
    }

    void setParamAttrDelim(vector<string> a_delim)
    {
        _attributeDelimiter = getParamDelim(a_delim);
    }

    char getParamDelim(vector<string> a_delim)
    {
        string delim = a_delim[0];
        char ret_delim;
        if (delim == "\\t")
        {
            ret_delim = '\t';
        }
        else if (delim == "\\r")
        {
            ret_delim = '\r';
        }
        else if (delim == "\\n")
        {
            ret_delim = '\n';
        }
        else if (delim == "")
        {
            ret_delim = ' ';
        }
        else
        {
            try
            {
                ret_delim = lexical_cast<char>(delim);
            }
            catch (bad_lexical_cast const& exn)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse delimiter";
            }
        }
        return ret_delim;
    }

    void setParamNullPattern(vector<string> nPattern)
    {
        string nullPattern = nPattern[0];
        _nullPrefix.resize(0);
        _nullPostfix.resize(0);
        _printNullCode = false;
        size_t c;
        for(c=0; c<nullPattern.size(); ++c)
        {
            if(nullPattern[c] != '%')
            {
                _nullPrefix.append(1, nullPattern[c]);
            }
            else
            {
                break;
            }
        }
        if(c<nullPattern.size() && nullPattern[c]=='%')
        {
            _printNullCode = true;
            ++c;
        }
        for( ; c<nullPattern.size(); ++c)
        {
            _nullPostfix.append(1, nullPattern[c]);
        }
    }

    void setParamPrecision(vector<int64_t> precis)
    {
        _precision = precis[0];
        if(_precision<=0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "precision must be positive";
        }
    }

    int64_t getParamContentInt64(Parameter& param)
    {
        size_t paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_INT64).getInt64();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getInt64();
            LOG4CXX_DEBUG(logger, "aio_save integer param is " << paramContent)

        }
        return paramContent;
    }

    bool setKeywordParamInt64(KeywordParameters const& kwParams, const char* const kw, void (AioSaveSettings::* innersetter)(vector<int64_t>) )
    {
        vector<int64_t> paramContent;
        size_t numParams;
        bool retSet = false;

        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            if (kwParam->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(kwParam.get());
                Parameters& gParams = group->getParameters();
                numParams = gParams.size();
                for (size_t i = 0; i < numParams; ++i) {
                    paramContent.push_back(getParamContentInt64(gParams[i]));
                }
            } else {
                paramContent.push_back(getParamContentInt64(kwParam));
            }
            (this->*innersetter)(paramContent);
            retSet = true;
        } else {
            LOG4CXX_DEBUG(logger, "aio_save findKeyword null: " << kw);
        }
        return retSet;
    }

    void setKeywordParamInt64(KeywordParameters const& kwParams, const char* const kw, bool& alreadySet, void (AioSaveSettings::* innersetter)(vector<int64_t>) )
    {
        checkIfSet(alreadySet, kw);
        alreadySet = setKeywordParamInt64(kwParams, kw, innersetter);
    }

    string getParamContentString(Parameter& param)
    {
        string paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_STRING).getString();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getString();
        }
        return paramContent;
    }

    bool setKeywordParamString(KeywordParameters const& kwParams, const char* const kw, void (AioSaveSettings::* innersetter)(vector<string>) )
    {
        vector <string> paramContent;
        bool retSet = false;

        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            if (kwParam->getParamType() == PARAM_NESTED) {
                auto group = dynamic_cast<OperatorParamNested*>(kwParam.get());
                Parameters& gParams = group->getParameters();
                for (size_t i = 0; i < gParams.size(); ++i) {
                    paramContent.push_back(getParamContentString(gParams[i]));
                }
            } else {
                paramContent.push_back(getParamContentString(kwParam));
            }
            (this->*innersetter)(paramContent);
            retSet = true;
        } else {
            LOG4CXX_DEBUG(logger, "aio_input findKeyword null: " << kw);
        }
        return retSet;
    }

    void setKeywordParamString(KeywordParameters const& kwParams, const char* const kw, bool& alreadySet, void (AioSaveSettings::* innersetter)(vector<string>) )
    {
        checkIfSet(alreadySet, kw);
        alreadySet = setKeywordParamString(kwParams, kw, innersetter);
    }

    bool getParamContentBool(Parameter& param)
    {
        bool paramContent;

        if(param->getParamType() == PARAM_LOGICAL_EXPRESSION) {
            ParamType_t& paramExpr = reinterpret_cast<ParamType_t&>(param);
            paramContent = evaluate(paramExpr->getExpression(), TID_BOOL).getBool();
        } else {
            OperatorParamPhysicalExpression* exp =
                dynamic_cast<OperatorParamPhysicalExpression*>(param.get());
            SCIDB_ASSERT(exp != nullptr);
            paramContent = exp->getExpression()->evaluate().getBool();
        }
        return paramContent;
    }

    void setKeywordParamBool(KeywordParameters const& kwParams, const char* const kw, bool& value)
    {
        Parameter kwParam = getKeywordParam(kwParams, kw);
        if (kwParam) {
            bool paramContent = getParamContentBool(kwParam);
            LOG4CXX_DEBUG(logger, "aio_input setting " << kw << " to " << paramContent);
            value = paramContent;
        } else {
            LOG4CXX_DEBUG(logger, "aio_input findKeyword null: " << kw);
        }
    }

    Parameter getKeywordParam(KeywordParameters const& kwp, const std::string& kw) const
    {
        auto const& kwPair = kwp.find(kw);
        return kwPair == kwp.end() ? Parameter() : kwPair->second;
    }

public:
    static const size_t MAX_PARAMETERS = 6;

    AioSaveSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                    KeywordParameters const& kwParams,
                    bool logical,
                    shared_ptr<Query>& query):
                _bufferSize(8 * 1024 * 1024),
                _cellsPerChunk(-1),
                _attributeDelimiter('\t'),
                _lineDelimiter('\n'),
                _numInstances(query->getInstancesCount()),
                _format(TEXT),
                _binaryFormatString(""),
                _nullPrefix("\\N"),
                _printNullCode(false),
                _nullPostfix(""),
                _printCoordinates(false),
                _quoteStrings(false),
                _writeHeader(false),
                _precision(std::numeric_limits<double>::digits10),
                _attsOnly(true),
                _resultSizeLimit(-1),
                _usingCsvPlus(false)
    {
        string const instanceHeader                = "instance=";
        string const instancesHeader               = "instances=";
        size_t const nParams = operatorParameters.size();
        bool  cellsPerChunkSet      = false;
        bool  bufferSizeSet         = false;
        bool  attributeDelimiterSet = false;
        bool  lineDelimiterSet      = false;
        bool  formatSet             = false;
        bool  nullPatternSet        = false;
        bool  resultSizeLimitSet        = false;
        if(_precision <= 0)
        {//correct for an unfortunate configuration problem that may arise
            _precision = 6;
        }
        bool  precisionSet          = false;


        setKeywordParamInt64(kwParams, KW_CELLS_PER_CHUNK, cellsPerChunkSet, &AioSaveSettings::setParamCellsPerChunk);
        setKeywordParamInt64(kwParams, KW_BUF_SZ, bufferSizeSet, &AioSaveSettings::setParamBufferSize);
        setKeywordParamString(kwParams, KW_LINE_DELIM, lineDelimiterSet, &AioSaveSettings::setParamLineDelim);
        setKeywordParamString(kwParams, KW_ATTR_DELIM, attributeDelimiterSet, &AioSaveSettings::setParamAttrDelim);
        setKeywordParamString(kwParams, KW_FORMAT, formatSet, &AioSaveSettings::setParamFormat);
        setKeywordParamString(kwParams, KW_NULL_PATTERN, nullPatternSet, &AioSaveSettings::setParamNullPattern);
        setKeywordParamInt64(kwParams, KW_PRECISION, precisionSet, &AioSaveSettings::setParamPrecision);
        setKeywordParamInt64(kwParams, KW_RESULT_LIMIT, resultSizeLimitSet, &AioSaveSettings::setParamResultSizeLimit);
        setKeywordParamBool(kwParams, KW_ATTS_ONLY, _attsOnly);
        setKeywordParamString(kwParams, KW_PATHS, &AioSaveSettings::setParamPaths);
        setKeywordParamInt64(kwParams, KW_INSTANCES, &AioSaveSettings::setParamInstances);

        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to UnparseSettings";
        }

        if (nParams > 0)
        {
            shared_ptr<OperatorParam>const& param = operatorParameters[0];
            string parameterString;
            if (logical)
            {
                parameterString = evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(), TID_STRING).getString();
            }
            else
            {
                parameterString = ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
            }
            if(_filePaths.size())
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal attempt to set path multiple times";
            }
            string paramContent = parameterString;
            trim(paramContent);
            _filePaths.push_back(path::expandForSave(paramContent, *query));
        }
        if(_filePaths.size() == 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "file path(s) was not provided, or failed to parse";
        }
        if(_instanceIds.size() == 0)
        {
            _instanceIds.push_back(query->getPhysicalCoordinatorID(true));
        }
        if(_filePaths.size() != _instanceIds.size())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "the number of file paths provided does not match the number of instance IDs";
        }
        set<InstanceID> uniqueInstances;
        for(size_t i =0; i<_instanceIds.size(); ++i)
        {
            uniqueInstances.insert(_instanceIds[i]);
        }
        if(uniqueInstances.size() < _instanceIds.size())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "the provided instance IDs are not unique";
        }
        for(size_t i=0; i< _filePaths.size(); ++i)
        {
            InstanceID const iid = _instanceIds[i];
            if(query->isPhysicalInstanceDead(iid))
            {
                ostringstream err;
                err<<"Instance "<<iid<<" is not currently part of the cluster";
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str().c_str();
            }
            InstanceID logId = query->mapPhysicalToLogical(iid);
            _instancesAndPaths[logId] = _filePaths[i];
        }
        if((_format == BINARY || _usingCsvPlus) && (lineDelimiterSet || attributeDelimiterSet || nullPatternSet))
        {
            LOG4CXX_DEBUG(logger, "line delimiter: " << lineDelimiterSet);
            LOG4CXX_DEBUG(logger, "att delimiter: " << attributeDelimiterSet);
            LOG4CXX_DEBUG(logger, "null pattern: " << nullPatternSet);
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "attribute_delimiter, line_delimiter and null_pattern are only used with 'format=tdv'";
        }
        if(_usingCsvPlus)
        {
            _nullPrefix = "null";
            _quoteStrings = true;
            _printCoordinates=true;
            _attributeDelimiter = ',';
            _lineDelimiter = '\n';
            _writeHeader = true;
        }
    }

    int64_t getCellsPerChunk() const
    {
        return _cellsPerChunk;
    }

    size_t getBufferSize() const
    {
        return _bufferSize;
    }

    char getAttributeDelimiter() const
    {
        return _attributeDelimiter;
    }

    char getLineDelimiter() const
    {
        return _lineDelimiter;
    }

    bool isBinaryFormat() const
    {
        return _format == BINARY;
    }

    bool isArrowFormat() const
    {
        return _format == ARROW;
    }

    bool isAttsOnly() const
    {
        return _attsOnly;
    }

    string const& getBinaryFormatString() const
    {
        return _binaryFormatString;
    }

    map<InstanceID, string> const& getInstanceMap() const
    {
        return _instancesAndPaths;
    }

    inline void printNull(ostringstream& output, int8_t missingReason) const
    {
        output<<_nullPrefix;
        if(_printNullCode)
        {
            output<<(int64_t)missingReason<<_nullPostfix;
        }
        else
        {
            return;
        }
    }

    bool printCoordinates() const
    {
        return _printCoordinates;
    }

    bool quoteStrings() const
    {
        return _quoteStrings;
    }

    bool printHeader() const
    {
        return _writeHeader;
    }

    int32_t getPrecision() const
    {
        return _precision;
    }

    size_t getResultSizeLimit() const
    {
        size_t retVal = _resultSizeLimit;
        if (_resultSizeLimit > -1) {
            retVal = retVal * 1024 * 1024;
        }
        return retVal;
    }
};

}


#endif //UnparseSettings
