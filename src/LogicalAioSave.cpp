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
        AioSaveSettings settings (_parameters, true, query);
        vector<DimensionDesc> dimensions(3);
        size_t const nInstances = query->getInstancesCount();
        dimensions[0] = DimensionDesc("chunk_no",    0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        dimensions[1] = DimensionDesc("dest_instance_id",   0, 0, nInstances-1, nInstances-1, 1, 0);
        dimensions[2] = DimensionDesc("source_instance_id", 0, 0, nInstances-1, nInstances-1, 1, 0);
        vector<AttributeDesc> attributes;
        attributes.push_back(AttributeDesc((AttributeID)0, "val", TID_STRING, AttributeDesc::IS_NULLABLE, 0));
        return ArrayDesc("aio_save", attributes, dimensions, defaultPartitioning());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalAioSave, "aio_save");

} // emd namespace scidb
