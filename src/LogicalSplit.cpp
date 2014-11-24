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
        //ADD_PARAM_VARIES();
    }

//    std::vector<boost::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector< ArrayDesc> &schemas)
//    {
//
//        std::vector<boost::shared_ptr<OperatorParamPlaceholder> > res;
//        res.push_back(END_OF_VARIES_PARAMS());
//        switch (_parameters.size()) {
//        case 0:
//        case 1:
//            res.push_back(PARAM_CONSTANT("string"));
//            break;
//        default:
//            break;
//        }
//        return res;
//    }

    ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, boost::shared_ptr< Query> query)
    {
        vector<AttributeDesc> attributes(1);
        attributes[0] = AttributeDesc((AttributeID)0, "foobar",  TID_STRING, 0, 0);
        vector<DimensionDesc> dimensions(1);
        dimensions[0] = DimensionDesc("i", 0, 0, MAX_COORDINATE, MAX_COORDINATE, 1, 0);
        return ArrayDesc("split", attributes, dimensions);
    }

};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalSplit, "split");

} // emd namespace scidb
