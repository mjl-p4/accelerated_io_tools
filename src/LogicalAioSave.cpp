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

#include "AioSaveSettings.h"

namespace scidb
{

class LogicalAioSave : public  LogicalOperator
{
public:
    LogicalAioSave(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_VARIES();
    }

    //unparse(array,'format=tsv','lines_per_chunk=10')
    //returns <val:string> [iid=0:*,1,0 chunk=0:*,1,0]
    std::vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < AioSaveSettings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT("string"));
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        Attributes inputAttributes   = inputSchema.getAttributes(true);

        AioSaveSettings settings (_parameters, true, query);
        vector<DimensionDesc> dimensions(2);

#ifdef CPP11
        dimensions[0] = DimensionDesc("source_instance_id", 0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        dimensions[1] = DimensionDesc("chunk_no",    0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
#else
        dimensions[0] = DimensionDesc("source_instance_id", 0, 0, MAX_COORDINATE, MAX_COORDINATE, 1, 0);
        dimensions[1] = DimensionDesc("chunk_no",           0, 0, MAX_COORDINATE, MAX_COORDINATE, 1, 0);
#endif
        vector<AttributeDesc> attributes;
        attributes.push_back(AttributeDesc((AttributeID)0, "val", TID_STRING, AttributeDesc::IS_NULLABLE, 0));
        //attributes = addEmptyTagAttribute(attributes);

#ifdef CPP11
        return ArrayDesc("aio_save", attributes, dimensions, defaultPartitioning());
#else
        return ArrayDesc("aio_save", attributes, dimensions);
#endif
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalAioSave, "aio_save");

} // emd namespace scidb
