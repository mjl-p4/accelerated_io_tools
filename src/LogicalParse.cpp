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
#include "ParseSettings.h"

namespace scidb
{

class LogicalParse : public  LogicalOperator
{
public:
    LogicalParse(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
        ADD_PARAM_INPUT();
        ADD_PARAM_VARIES();
    }

    std::vector<shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
    {
        std::vector<shared_ptr<OperatorParamPlaceholder> > res;
        res.push_back(END_OF_VARIES_PARAMS());
        if (_parameters.size() < ParseSettings::MAX_PARAMETERS)
        {
            res.push_back(PARAM_CONSTANT("string"));
        }
        return res;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        ArrayDesc const& inputSchema = schemas[0];
        Attributes inputAttributes = inputSchema.getAttributes(true);
        if (inputAttributes.size() != 1 ||
            inputAttributes[0].getType() != TID_STRING ||
            inputAttributes[0].getFlags() != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "input to parse must have a single, non-nullable string attribute";
        }
        if (inputSchema.getDimensions().size() != 2 ||
            inputSchema.getDimensions()[0].getStartMin() != 0 ||
            inputSchema.getDimensions()[0].getChunkInterval() != 1 ||
            inputSchema.getDimensions()[1].getStartMin() != 0 ||
            inputSchema.getDimensions()[1].getChunkInterval() != 1)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "input to parse must does not have the correct dimensions (2D, chunk size 1 each)";
        }
        ParseSettings settings (_parameters, true, query);
        size_t numRequestedAttributes = settings.getNumAttributes();
        size_t requestedChunkSize = settings.getChunkSize();
        vector<DimensionDesc> dimensions(3);

#ifdef CPP11
        dimensions[0] = DimensionDesc("source_instance_id", 0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        dimensions[1] = DimensionDesc("chunk_no",    0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        dimensions[2] = DimensionDesc("line_no",     0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), requestedChunkSize, 0); 
#else
        dimensions[0] = DimensionDesc("source_instance_id", 0, 0, MAX_COORDINATE, MAX_COORDINATE, 1, 0);
        dimensions[1] = DimensionDesc("chunk_no",           0, 0, MAX_COORDINATE, MAX_COORDINATE, 1, 0);
        dimensions[2] = DimensionDesc("line_no",            0, 0, MAX_COORDINATE, MAX_COORDINATE, requestedChunkSize, 0);
#endif
        vector<AttributeDesc> attributes;
        if (settings.getSplitOnDimension())
        {   //add 1 for the error column
            dimensions.push_back(DimensionDesc("attribute_no", 0, 0, numRequestedAttributes, numRequestedAttributes, numRequestedAttributes+1, 0));
            attributes.push_back(AttributeDesc(0, "a", TID_STRING, AttributeDesc::IS_NULLABLE, 0));
        }
        else
        {
            for(size_t i=0, n=numRequestedAttributes; i<n; ++i)
            {
                ostringstream attname;
                attname<<"a";
                attname<<i;
                attributes.push_back(AttributeDesc((AttributeID)i, attname.str(),  TID_STRING, AttributeDesc::IS_NULLABLE, 0));
            }
            attributes.push_back(AttributeDesc((AttributeID)numRequestedAttributes, "error", TID_STRING, AttributeDesc::IS_NULLABLE, 0));
        }
        attributes = addEmptyTagAttribute(attributes);
#ifdef CPP11
        return ArrayDesc("parse", attributes, dimensions, defaultPartitioning());
#else
        return ArrayDesc("parse", attributes, dimensions);
#endif
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalParse, "parse");

} // emd namespace scidb
