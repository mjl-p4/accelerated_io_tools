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

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <query/LogicalOperator.h>
#include <query/Query.h>
#include <query/Expression.h>
#include <util/PathUtils.h>

#ifndef AIO_INPUT_SETTINGS
#define AIO_INPUT_SETTINGS

using std::shared_ptr;
using boost::starts_with;
using boost::lexical_cast;
using boost::bad_lexical_cast;
using boost::algorithm::trim;
using namespace boost::filesystem;
using namespace std;

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.aio_input"));

namespace scidb
{
static const char* const KW_PATHS        = "paths";
static const char* const KW_INSTANCES    = "instances";
static const char* const KW_BUF_SZ       = "buffer_size";
static const char* const KW_HEADER       = "header";
static const char* const KW_LINE_DELIM 	 = "line_delimiter";
static const char* const KW_ATTR_DELIM 	 = "attribute_delimiter";
static const char* const KW_NUM_ATTR     = "num_attributes";
static const char* const KW_CHUNK_SZ     = "chunk_size";
static const char* const KW_SPLIT_ON_DIM = "split_on_dimension";
static const char* const KW_SKIP         = "skip";

typedef std::shared_ptr<OperatorParamLogicalExpression> ParamType_t ;

class AioInputSettings
{
public:

    /**
     * Used to indicate if aio_input should, with respect to
     * the 'error' attribute:
     *   - skip NOTHING: pass all lines from input through.
     *   - skip ERRORS: pass only lines from input where the
     *     'error' attribute is null.
     *   - skip NON_ERRORS: pass only lines from input where
     *     the 'error' attribute is not null (useful for
     *     more quickly finding unexpected lines in the input).
     */
    enum class Skip {
        NOTHING = 0,
        ERRORS,
        NON_ERRORS
    };

private:

    bool             _singlepath;
    bool             _multiplepath;
    string           _inputFilePath;
    vector<string>   _inputPaths;
    vector<int64_t>  _inputInstances;
    bool             _thisInstanceReadsData;
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
    Skip             _skip;
    bool             _skipSet;

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
        for (size_t i = 0; i < paths.size(); ++i)
        {
            _inputPaths.push_back(paths[i]);
        }
        _multiplepath = true;
    }

    void setParamInstances(vector<int64_t> instances)
    {
        for (size_t i = 0; i < instances.size(); ++i) {
           _inputInstances.push_back(instances[i]);
        }
    }

    void setParamHeader(vector<int64_t> header)
    {
        if(header[0] < 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "header must be non negative";
        }
        _header = header[0];
    }

    void setParamBufferSize(vector<int64_t> buffer_size)
    {
        if(buffer_size[0] <=8 )
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "buffer_size must be greater than 8";
        }
        if(buffer_size[0] >= 1024*1024*1024)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "buffer_size must be under 1GB";
        }
        _bufferSize = buffer_size[0];
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

    void setParamNumAttr(vector<int64_t> atts)
    {
        if(atts[0] <= 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse num_attributes";
        }
        _numAttributes = atts[0];
    }

    void setParamChunkSize(vector<int64_t> chunk_size)
    {
        if(chunk_size[0] <= 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "chunk_size must be positive";
        }
        _chunkSize = chunk_size[0];
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
            LOG4CXX_DEBUG(logger, "aio_input integer param is " << paramContent)

        }
        return paramContent;
    }

    void setKeywordParamInt64(KeywordParameters const& kwParams, const char* const kw, bool& alreadySet, void (AioInputSettings::* innersetter)(vector<int64_t>) )
    {
        checkIfSet(alreadySet, kw);

        vector<int64_t> paramContent;
        size_t numParams;

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
            alreadySet = true;
        } else {
            LOG4CXX_DEBUG(logger, "aio_input findKeyword null: " << kw);
        }
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

    void setKeywordParamString(KeywordParameters const& kwParams, const char* const kw, bool& alreadySet, void (AioInputSettings::* innersetter)(vector<string>) )
    {
        checkIfSet(alreadySet, kw);
        vector <string> paramContent;

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
            alreadySet = true;
        } else {
            LOG4CXX_DEBUG(logger, "aio_input findKeyword null: " << kw);
        }
    }

    Parameter getKeywordParam(KeywordParameters const& kwp, const std::string& kw) const
    {
        auto const& kwPair = kwp.find(kw);
        return kwPair == kwp.end() ? Parameter() : kwPair->second;
    }

    void setSkip(vector<string> args)
    {
        const auto& skipOp = args[0];

        if (skipOp == "nothing") {
            _skip = AioInputSettings::Skip::NOTHING;
        }
        else if (skipOp == "errors") {
            _skip = AioInputSettings::Skip::ERRORS;
        }
        else if (skipOp == "non-errors") {
            _skip = AioInputSettings::Skip::NON_ERRORS;
        }
        else {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                << "'skip' may be:  'nothing' (pass everything), 'errors' (skip errors), "
                "or 'non-errors' (skip non-errors)";
        }
    }

public:
    static const size_t MAX_PARAMETERS = 1;

    AioInputSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                     KeywordParameters const& kwParams,
                     bool logical,
                     shared_ptr<Query>& query):
       _singlepath(false),
       _multiplepath(false),
       _inputFilePath(""),
       _thisInstanceReadsData(false),
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
       _splitOnDimensionSet(false),
       _skip(AioInputSettings::Skip::NOTHING),
       _skipSet(false)
    {
        bool pathsSet = false;
        bool instancesSet = false;
        bool numAttrsSet = false;
        int64_t const myLogicalInstanceId = query->getInstanceID();
        InstanceID const myPhysicalInstanceId = query->getPhysicalInstanceID();
        size_t const nParams = operatorParameters.size();
        string parameterString;

        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "illegal number of parameters passed to AioInputSettings";
        }
        if (nParams > 0)
        {
            shared_ptr<OperatorParam>const& param = operatorParameters[0];
            if (logical)
            {
                parameterString =
                    evaluate(((shared_ptr<OperatorParamLogicalExpression>&) param)->getExpression(), TID_STRING).getString();
            }
            else
            {
                parameterString =
                    ((shared_ptr<OperatorParamPhysicalExpression>&) param)->getExpression()->evaluate().getString();
            }
        }

        setKeywordParamString(kwParams, KW_PATHS, pathsSet, &AioInputSettings::setParamPaths);
        setKeywordParamInt64(kwParams, KW_INSTANCES, instancesSet, &AioInputSettings::setParamInstances);
        setKeywordParamInt64(kwParams, KW_HEADER, _headerSet, &AioInputSettings::setParamHeader);
        setKeywordParamInt64(kwParams, KW_BUF_SZ, _bufferSizeSet, &AioInputSettings::setParamBufferSize);
        setKeywordParamString(kwParams, KW_LINE_DELIM, _lineDelimiterSet, &AioInputSettings::setParamLineDelim);
        setKeywordParamString(kwParams, KW_ATTR_DELIM, _attributeDelimiterSet, &AioInputSettings::setParamAttrDelim);
        setKeywordParamInt64(kwParams, KW_NUM_ATTR, numAttrsSet, &AioInputSettings::setParamNumAttr);
        setKeywordParamBool(kwParams, KW_SPLIT_ON_DIM, _splitOnDimension);
        setKeywordParamInt64(kwParams, KW_CHUNK_SZ, _chunkSizeSet, &AioInputSettings::setParamChunkSize);
        setKeywordParamString(kwParams, KW_SKIP, _skipSet, &AioInputSettings::setSkip);

        for (size_t i= 0; i<nParams; ++i)
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
            _inputFilePath  = path::expandForRead(path, *query);
            _thisInstanceReadsData = query->isCoordinator();
        }
        // multipath vs single path actually doesn't mean there is one file specified, it refers to whether a
        // positional argument is used (which can only specify one path), or the keywords parameter is used.
        // If a single path is specified using the keyword 'paths:', the code path should still be
        // _multipath = true.
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
            if((_inputInstances.size()==1) && _inputInstances[0]==-1)
            {
                 string relinputpath  = _inputPaths[0];
                 _inputFilePath  = relinputpath;
                 _thisInstanceReadsData = true;
            }
            else
            {
                for(size_t i=0; i<_inputInstances.size(); ++i)
                {
                    InstanceID physId = _inputInstances[i];
                    if(query->isPhysicalInstanceDead(physId))
                    {
                        ostringstream err;
                        err<<"Physical instance "<<physId<<" is not alive at the moment";
                        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str().c_str();
                    }
                    if(physId == myPhysicalInstanceId)
                    {
                        _thisInstanceReadsData = true;
                        _inputFilePath       = _inputPaths[i];
                    }
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

    bool thisInstanceReadsData()
    {
        return _thisInstanceReadsData;
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

    Skip getSkip() const
    {
        return _skip;
    }
};

}

#endif //UBER_LOAD_SETTINGS
