/*
**
* BEGIN_COPYRIGHT
*
* dmetric is a plugin for SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* dmetric is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* dmetric is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
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
    double _threshold;
    bool   _thresholdSet;

public:
    static const size_t MAX_PARAMETERS = 1;

    SplitSettings(vector<shared_ptr<OperatorParam> > const& operatorParameters,
                 bool logical,
                 shared_ptr<Query>& query):
       _threshold(0),
       _thresholdSet(false)
    {
        string const thresholdParamHeader        = "threshold=";
        size_t const nParams = operatorParameters.size();
        if (nParams > MAX_PARAMETERS)
        {   //assert-like exception. Caller should have taken care of this!
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                  << "illegal number of parameters passed to SplitSettings";
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
            if (starts_with(parameterString, thresholdParamHeader))
            {
                string paramContent = parameterString.substr(thresholdParamHeader.size());
                trim(paramContent);
                try
                {
                    _threshold = lexical_cast<double>(paramContent);
                    _thresholdSet = true;
                }
                catch (bad_lexical_cast const& exn)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "could not parse threshold";
                }
            }
            else
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_OPERATOR, SCIDB_LE_UNRECOGNIZED_PARAMETER) << parameterString;
            }
        }
    }

    bool isThresholdSet() const
    {
        return _thresholdSet;
    }

    double getThreshold() const
    {
        return _threshold;
    }
};

}

#endif //SPLIT_SETTINGS
