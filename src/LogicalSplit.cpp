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

    std::vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < SplitSettings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT("string"));
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        SplitSettings settings (_parameters, true, query); //construct and check to ensure settings are legit
        vector<AttributeDesc> attributes(1);
        attributes[0] = AttributeDesc((AttributeID)0, "value",  TID_STRING, 0, 0);
        vector<DimensionDesc> dimensions(2);
#ifdef CPP11
        dimensions[0] = DimensionDesc("source_instance_id", 0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        dimensions[1] = DimensionDesc("chunk_no",    0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        return ArrayDesc("split", attributes, dimensions,
                         defaultPartitioning());
#else
        dimensions[0] = DimensionDesc("source_instance_id", 0, 0, MAX_COORDINATE, MAX_COORDINATE, 1, 0);
        dimensions[1] = DimensionDesc("chunk_no",    0, 0, MAX_COORDINATE, MAX_COORDINATE, 1, 0);
        return ArrayDesc("split", attributes, dimensions);
#endif
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalSplit, "split");

} // emd namespace scidb
