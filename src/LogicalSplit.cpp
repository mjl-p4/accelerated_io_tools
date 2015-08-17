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

#include <query/Operator.h>
#include "SplitSettings.h"

namespace scidb
{

class LogicalSplit : public  LogicalOperator
{
public:
    LogicalSplit(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_VARIES();
    }

    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < SplitSettings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT("string"));
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        SplitSettings settings (_parameters, true, query); //construct and check to ensure settings are legit
        vector<AttributeDesc> attributes(1);
        attributes[0] = AttributeDesc((AttributeID)0, "value",  TID_STRING, 0, 0);
        vector<DimensionDesc> dimensions(2);
        dimensions[0] = DimensionDesc("source_instance_id", 0, 0, MAX_COORDINATE, MAX_COORDINATE, 1, 0);
        dimensions[1] = DimensionDesc("chunk_no",    0, 0, MAX_COORDINATE, MAX_COORDINATE, 1, 0);
        return ArrayDesc("split", attributes, dimensions);
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalSplit, "genx");

} // emd namespace scidb
