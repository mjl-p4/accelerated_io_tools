
#include <limits>
#include <limits>
#include <sstream>

#include <boost/unordered_map.hpp>
#include <query/Operator.h>
#include <util/Platform.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <system/Sysinfo.h>
#include "ParseSettings.h"

namespace scidb
{
using namespace boost;
using namespace scidb;


class PhysicalParse : public PhysicalOperator
{
public:
    PhysicalParse(std::string const& logicalName,
                     std::string const& physicalName,
                     Parameters const& parameters,
                     ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        shared_ptr<Array> result = boost::shared_ptr<MemArray>(new MemArray(_schema,query));
        return result;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalParse, "parse", "PhysicalParse");


} // end namespace scidb
