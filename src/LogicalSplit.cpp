/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
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

#include <query/LogicalOperator.h>
#include <query/OperatorParam.h>
#include "SplitSettings.h"

namespace scidb
{

class LogicalSplit : public  LogicalOperator
{
public:
    LogicalSplit(const std::string& logicalName, const std::string& alias):
        LogicalOperator(logicalName, alias)
    {
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(RE::STAR, {
                 RE(PP(PLACEHOLDER_CONSTANT, TID_STRING))
              })
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        SplitSettings settings (_parameters, true, query); //construct and check to ensure settings are legit
        //vector<AttributeDesc> attributes(1);
        Attributes attributes;
        attributes.push_back(AttributeDesc((AttributeID)0, "value",  TID_STRING, 0, CompressorType::NONE));
        vector<DimensionDesc> dimensions(2);

        dimensions[0] = DimensionDesc("source_instance_id", 0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        dimensions[1] = DimensionDesc("chunk_no",    0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        return ArrayDesc("split", attributes, dimensions, createDistribution(defaultPartitioning()), query->getDefaultArrayResidency());
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalSplit, "split");

} // emd namespace scidb
