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

#include <boost/algorithm/string.hpp>

namespace scidb
{
using namespace boost;
using namespace scidb;

class OutputWriter : public boost::noncopyable
{
private:
    shared_ptr<Array> const _output;
    Coordinates _outputPosition;
    size_t const _numLiveAttributes;
    char _lineDelimiter;
    char _attributeDelimiter;
    vector<Value> _outputLine;
    size_t const _outputChunkSize;
    vector<shared_ptr<ArrayIterator> > _outputArrayIterators;
    vector<shared_ptr<ChunkIterator> > _outputChunkIterators;

public:
    OutputWriter(ArrayDesc const& schema, shared_ptr<Query>& query, char lineDelimiter, char attributeDelimiter):
        _output(boost::make_shared<MemArray>(schema,query)),
        _outputPosition(3, 0),
        _numLiveAttributes(schema.getAttributes(true).size()),
        _lineDelimiter(lineDelimiter),
        _attributeDelimiter(attributeDelimiter),
        _outputLine(_numLiveAttributes),
        _outputChunkSize(schema.getDimensions()[2].getChunkInterval()),
        _outputArrayIterators(_numLiveAttributes),
        _outputChunkIterators(_numLiveAttributes)
    {
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputArrayIterators[i] = _output->getIterator(i);
        }
    }

    void processChunk(Coordinates const& inputChunkPosition, Value const& value, shared_ptr<Query>& query)
    {
        _outputPosition[0] = inputChunkPosition[0];
        _outputPosition[1] = inputChunkPosition[1];
        _outputPosition[2] = 0;
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputChunkIterators[i] = _outputArrayIterators[i]->newChunk(_outputPosition).getIterator(query,
                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
            _outputChunkIterators[i]->setPosition(_outputPosition);
        }
        const char *data = value.getString();
        if(data[ value.size() - 1] != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered a string that is not null-terminated; bailing";
        }
        string input(data);
        //vector <string> lines = boost::split(input, boost::is_any_of(_lineDelimiter));
        vector <string> lines;
        split(lines, input, is_from_range(_lineDelimiter, _lineDelimiter));
        size_t const nLines = lines.size();
        if (nLines > _outputChunkSize)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered a string with more lines than the chunk size; bailing";
        }
        for ( size_t l = 0; l<nLines; ++l)
        {
            string const& line = lines[l];
            vector<string> tokens;
            split(tokens, line, is_from_range(_attributeDelimiter, _attributeDelimiter));
            size_t const nTokens = tokens.size();
            //set the error string
            if(nTokens < _numLiveAttributes -1 )
            {
                _outputLine[_numLiveAttributes-1].setString("short");
            }
            else if (nTokens >_numLiveAttributes -1 )
            {
                ostringstream error;
                error<<"long";
                for(size_t t = _numLiveAttributes - 1; t<nTokens; ++t)
                {
                    error << _attributeDelimiter << tokens[t];
                }
                _outputLine[_numLiveAttributes-1].setString(error.str().c_str());
            }
            else
            {
                _outputLine[_numLiveAttributes-1].setNull();
            }
            for(size_t t= 0; t<_numLiveAttributes -1; ++t)
            {
                if(t<nTokens)
                {
                    _outputLine[t].setString(tokens[t].c_str());
                }
                else
                {
                    _outputLine[t].setNull();
                }
            }
            ++(_outputPosition[2]);
            for(AttributeID i =0; i<_numLiveAttributes; ++i)
            {
               _outputChunkIterators[i]->writeItem(_outputLine[i]);
               _outputChunkIterators[i]->setPosition(_outputPosition);
            }
        }
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputChunkIterators[i]->flush();
            _outputChunkIterators[i].reset();
        }
    }

    /**
     * Flush the last chunk and return the resulting array object. After this, the class is invalidated.
     * @return the output array
     */
    shared_ptr<Array> finalize()
    {
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputChunkIterators[i].reset();
            _outputArrayIterators[i].reset();
        }
        return _output;
    }
};

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
        ParseSettings settings (_parameters, false, query);
        OutputWriter writer(_schema, query, settings.getLineDelimiter(), settings.getAttributeDelimiter());
        shared_ptr<Array>& input = inputArrays[0];
        shared_ptr<ConstArrayIterator> inputIterator = input->getConstIterator(0);
        while(!inputIterator-> end())
        {
            Coordinates const& pos = inputIterator->getPosition();
            shared_ptr<ConstChunkIterator> inputChunkIterator = inputIterator->getChunk().getConstIterator();
            if(!inputChunkIterator->end()) //just 1 value in chunk
            {
                Value const& v = inputChunkIterator->getItem();
                writer.processChunk(pos, v, query);
            }
            ++(*inputIterator);
        }
        return writer.finalize();
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalParse, "parse", "PhysicalParse");


} // end namespace scidb
