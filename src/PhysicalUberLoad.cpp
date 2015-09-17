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
#include "UberLoadSettings.h"

#include <boost/algorithm/string.hpp>

using std::make_shared;

using boost::algorithm::is_from_range;

namespace scidb
{

using namespace scidb;

/**
 * A wrapper around an open file (or pipe) that may iterate over the data once and split it into blocks, each
 * block containing a number of lines. Returns one block at a time.
 */
class BinaryFileSplitter
{
private:
    size_t       _bufferSize;
    Value        _buffer;
    bool         _endOfFile;
    FILE*        _inputFile;

public:
    BinaryFileSplitter(string const& filePath,
                       size_t bufferSize,
                       int64_t header,
                       char lineDelimiter):
        _bufferSize(bufferSize),
        _endOfFile(false),
        _inputFile(0)
    {
        try
        {
            _buffer.setSize(_bufferSize);
        }
        catch(...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter() cannot allocate memory";
        }
        _inputFile = fopen(filePath.c_str(), "r");
        if (_inputFile == NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter() cannot open file";
        }
        if(header>0)
        {
            char *line = NULL;
            size_t linesize = 0;
            ssize_t nread = 0;
            for(int64_t j=0; j<header && nread>=0; ++j)
            {
                nread = getdelim(&line, &linesize, (int)lineDelimiter, _inputFile);
            }
            free(line);
        }
    }

    ~BinaryFileSplitter()
    {
        if(_inputFile!=0)
        {
            fclose(_inputFile);
        }
    }

    bool readMore()
    {
        if(_endOfFile)
        {
            return false;
        }
        size_t numBytes = fread(_buffer.data(), 1, _bufferSize, _inputFile);
        if(numBytes != _bufferSize)
        {
            _endOfFile = true;
            fclose(_inputFile);
            _inputFile = 0;
            if(numBytes == 0)
            {
                return false;
            }
            if(numBytes > sizeof(int64_t))
            {
                _buffer.setSize(numBytes);
            }
            else
            {
                vector<char>bb(numBytes, 0);
                memcpy(&(bb[0]), _buffer.data(), numBytes);
                _buffer.setSize(numBytes);
                memcpy(_buffer.data(), &(bb[0]), numBytes);
            }
        }
        return true;
    }

    Value const& getBuffer()
    {
        return _buffer;
    }
};

class BinEmptySinglePass : public SinglePassArray
{
private:
    typedef SinglePassArray super;
    MemChunk _dummy;

public:
    BinEmptySinglePass(ArrayDesc const& schema):
            super(schema)
    {
        super::setEnforceHorizontalIteration(true);
    }
    virtual ~BinEmptySinglePass()
    {}
    size_t getCurrentRowIndex() const
    {
        return 0;
    }
    bool moveNext(size_t rowIndex)
    {
        return false;
    }
    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        return _dummy;
    }
};

class BinFileSplitArray : public SinglePassArray
{
private:
    typedef SinglePassArray super;
    size_t _rowIndex;
    Address _chunkAddress;
    MemChunk _chunk;
    weak_ptr<Query> _query;
    BinaryFileSplitter _splitter;
    char const*  _buffer;
    size_t _bufferSize;

public:
    BinFileSplitArray(ArrayDesc const& schema,
                      shared_ptr<Query>& query,
                      shared_ptr<UberLoadSettings> settings):
        super(schema),
        _rowIndex(0),
        _chunkAddress(0, Coordinates(2,0)),
        _query(query),
        _splitter(settings->getInputFilePath(),
                  settings->getBlockSize(),
                  settings->getHeader(),
                  settings->getLineDelimiter())
    {
        super::setEnforceHorizontalIteration(true);
        _chunkAddress.coords[0] = settings->getParseInstance();
    }

    virtual ~BinFileSplitArray()
    {}

    size_t getCurrentRowIndex() const
    {
        return _rowIndex;
    }

    bool moveNext(size_t rowIndex)
    {
        bool res = _splitter.readMore();
        if (res)
        {
            ++_rowIndex;
        }
        return res;
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        _chunkAddress.coords[1] = _rowIndex  -1;
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        _chunk.initialize(this, &super::getArrayDesc(), _chunkAddress, 0);
        shared_ptr<ChunkIterator> chunkIt = _chunk.getIterator(query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        chunkIt->writeItem(_splitter.getBuffer());
        chunkIt->flush();
        return _chunk;
    }
};


class OutputWriter : public boost::noncopyable
{
private:
    shared_ptr<Array> const _output;
    Coordinates _outputPosition;
    size_t const _numLiveAttributes;
    size_t const _outputLineSize;
    size_t const _outputChunkSize;
    vector<shared_ptr<ArrayIterator> > _outputArrayIterators;
    vector<shared_ptr<ChunkIterator> > _outputChunkIterators;
    bool _splitOnDimension;
    size_t _outputColumn;
    char const _attributeDelimiter;
    ostringstream _errorBuf;
    Value _buf;

public:
    OutputWriter(ArrayDesc const& schema, shared_ptr<Query>& query, bool splitOnDimension, char const attDelimiter):
        _output(make_shared<MemArray>(schema,query)),
        _outputPosition( splitOnDimension ? 4 : 3, 0),
        _numLiveAttributes(schema.getAttributes(true).size()),
        _outputLineSize(splitOnDimension ? schema.getDimensions()[3].getChunkInterval() : _numLiveAttributes),
        _outputChunkSize(schema.getDimensions()[2].getChunkInterval()),
        _outputArrayIterators(_numLiveAttributes),
        _outputChunkIterators(_numLiveAttributes),
        _splitOnDimension(splitOnDimension),
        _outputColumn(0),
        _attributeDelimiter(attDelimiter)
    {
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputArrayIterators[i] = _output->getIterator(i);
        }
    }

    void newChunk (Coordinates const& inputChunkPosition, shared_ptr<Query>& query)
    {
        _outputPosition[0] = inputChunkPosition[0];
        _outputPosition[1] = inputChunkPosition[1];
        _outputPosition[2] = 0;
        if(_splitOnDimension)
        {
            _outputPosition[3] = 0;
        }
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            if(_outputChunkIterators[i].get())
            {
                _outputChunkIterators[i]->flush();
            }
            _outputChunkIterators[i] = _outputArrayIterators[i]->newChunk(_outputPosition).getIterator(query,
                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        }
    }

    void writeValue (char const* start, char const* end)
    {
        if(_outputColumn < _outputLineSize - 1)
        {
            _buf.setSize(end - start + 1);
            char* d = _buf.getData<char>();
            memcpy(d, start, end-start);
            d[(end-start)]=0;

            //_buf.setString(value);
            if(_splitOnDimension)
            {
                _outputChunkIterators[0] -> setPosition(_outputPosition);
                _outputChunkIterators[0] -> writeItem(_buf);
                ++(_outputPosition[3]);
            }
            else
            {
                _outputChunkIterators[_outputColumn] -> setPosition(_outputPosition);
                _outputChunkIterators[_outputColumn] -> writeItem(_buf);
            }
        }
        else if (_outputColumn == _outputLineSize - 1)
        {
            string value(start, end-start);
            _errorBuf << "long" << _attributeDelimiter << value;
        }
        else
        {
            string value(start, end-start);
            _errorBuf << _attributeDelimiter << value;
        }
        ++_outputColumn;
    }

    void endLine()
    {
        if(_outputColumn < _outputLineSize - 1)
        {
            _buf.setNull();
            if(_splitOnDimension)
            {
                while (_outputColumn < _outputLineSize - 1)
                {
                    _outputChunkIterators[0] -> setPosition(_outputPosition);
                    _outputChunkIterators[0] -> writeItem(_buf);
                    ++(_outputPosition[3]);
                    ++_outputColumn;
                }
            }
            else
            {
                while (_outputColumn < _outputLineSize - 1)
                {
                    _outputChunkIterators[_outputColumn] -> setPosition(_outputPosition);
                    _outputChunkIterators[_outputColumn] -> writeItem(_buf);
                    ++_outputColumn;
                }
            }
            _errorBuf << "short";
        }
        if(_errorBuf.str().size())
        {
            _buf.setString(_errorBuf.str());
        }
        else
        {
            _buf.setNull();
        }
        if(_splitOnDimension)
        {
            _outputChunkIterators[0] -> setPosition(_outputPosition);
            _outputChunkIterators[0] -> writeItem(_buf);
            _outputPosition[3] = 0;
        }
        else
        {
            _outputChunkIterators[_outputLineSize - 1] -> setPosition(_outputPosition);
            _outputChunkIterators[_outputLineSize - 1] -> writeItem(_buf);
        }
        ++(_outputPosition[2]);
        _errorBuf.str("");
        _outputColumn = 0;
    }

    shared_ptr<Array> finalize()
    {
        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            if(_outputChunkIterators[i].get())
            {
                _outputChunkIterators[i]->flush();
            }
            _outputChunkIterators[i].reset();
            _outputArrayIterators[i].reset();
        }
        return _output;
    }
};

class PhysicalUberLoad : public PhysicalOperator
{
public:
    static ArrayDesc getSplitSchema()
    {
        vector<DimensionDesc> dimensions(2);
        dimensions[0] = DimensionDesc("source_instance_id", 0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        dimensions[1] = DimensionDesc("block_no",           0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        vector<AttributeDesc> attributes;
        attributes.push_back(AttributeDesc((AttributeID)0, "value",  TID_BINARY, 0, 0));
        return ArrayDesc("uber_load", attributes, dimensions, defaultPartitioning());
    }

    PhysicalUberLoad(std::string const& logicalName,
                     std::string const& physicalName,
                     Parameters const& parameters,
                     ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const&, std::vector<ArrayDesc> const&) const
    {
        return RedistributeContext(psUndefined);
    }

    shared_ptr<Array> makeSupplement(shared_ptr<Array>& afterSplit, shared_ptr<Query>& query, shared_ptr<UberLoadSettings>& settings)
    {
        char const lineDelim = settings->getLineDelimiter();
        shared_ptr<Array> supplement(new MemArray(getSplitSchema(), query));
        shared_ptr<ConstArrayIterator> srcArrayIter = afterSplit->getConstIterator(0);
        shared_ptr<ArrayIterator> dstArrayIter = supplement->getIterator(0);
        shared_ptr<ChunkIterator> dstChunkIter;
        while(!srcArrayIter->end())
        {
           Coordinates supplementCoords = srcArrayIter->getPosition();
           if(supplementCoords[1] != 0)
           {
               shared_ptr<ConstChunkIterator> srcChunkIter = srcArrayIter->getChunk().getConstIterator();
               Value const& v= srcChunkIter->getItem();
               supplementCoords[1]--;
               char const* start = static_cast<char const*>(v.data());
               char const* end = start;
               char const* lim = start + v.size();
               while( end != lim && (*end) != lineDelim)
               {
                   ++end;
               }
               if(end == lim)
               {
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered a whole block without line delim characters; Sorry! You need to increase the block size.";
               }
               Value firstLine;
               firstLine.setSize(end-start);
               memcpy(firstLine.data(), start, end-start);
               dstChunkIter = dstArrayIter->newChunk(supplementCoords).getIterator(query,  ChunkIterator::SEQUENTIAL_WRITE);
               dstChunkIter->writeItem(firstLine);
               dstChunkIter->flush();
           }
           ++(*srcArrayIter);
        }
        return supplement;
    }

    shared_ptr< Array> execute(std::vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        shared_ptr<UberLoadSettings> settings (new UberLoadSettings(_parameters, false, query));
        shared_ptr<Array> splitData;
        if(settings->getParseInstance() == static_cast<int64_t>(query->getInstanceID()))
        {
            splitData = shared_ptr<BinFileSplitArray>(new BinFileSplitArray(getSplitSchema(), query, settings));
        }
        else
        {
            splitData = shared_ptr<BinEmptySinglePass>(new BinEmptySinglePass(getSplitSchema()));
        }
        splitData = redistributeToRandomAccess(splitData, query, defaultPartitioning(),
                                                 ALL_INSTANCE_MASK,
                                                 std::shared_ptr<CoordinateTranslator>(),
                                                 0,
                                                 std::shared_ptr<PartitioningSchemaData>());
        shared_ptr<Array> supplement = makeSupplement(splitData, query, settings);
        supplement = redistributeToRandomAccess(supplement, query, defaultPartitioning(),
                                                ALL_INSTANCE_MASK,
                                                std::shared_ptr<CoordinateTranslator>(),
                                                0,
                                                std::shared_ptr<PartitioningSchemaData>());
        shared_ptr<ConstArrayIterator> inputIterator = splitData->getConstIterator(0);
        shared_ptr<ConstArrayIterator> supplementIter = supplement->getConstIterator(0);
        size_t const outputChunkSize = _schema.getDimensions()[2].getChunkInterval();
        char const attDelim = settings->getAttributeDelimiter();
        char const lineDelim = settings->getLineDelimiter();
        OutputWriter writer(_schema, query, settings->getSplitOnDimension(), settings->getAttributeDelimiter());
        while(!inputIterator-> end())
        {
            Coordinates const& pos = inputIterator->getPosition();
            shared_ptr<ConstChunkIterator> inputChunkIterator = inputIterator->getChunk().getConstIterator();
            if(!inputChunkIterator->end()) //just 1 value in chunk
            {
                size_t nLines =0;
                writer.newChunk(pos, query);
                Value const& v = inputChunkIterator->getItem();
                char* sourceStart = (char*) v.data();
                size_t sourceSize = v.size();
                if(pos[1]!=0)
                {
                    while((*sourceStart)!=lineDelim)
                    {
                        sourceStart ++;
                    }
                    sourceStart ++;
                    sourceSize = sourceSize - (sourceStart - ((char*)v.data()));
                }
                bool haveSupplement = supplementIter->setPosition(pos);
                vector<char>buf;
                if(haveSupplement)
                {
                    shared_ptr<ConstChunkIterator> supplementChunkIterator = supplementIter->getChunk().getConstIterator();
                    Value const &s = supplementChunkIterator->getItem();
                    buf.resize(sourceSize+s.size());
                    memcpy(&buf[0], sourceStart, sourceSize);
                    memcpy(&buf[0]+ sourceSize, s.data(), s.size());
                }
                else
                {
                    buf.resize(sourceSize);
                    memcpy(&buf[0], sourceStart, sourceSize);
                }
                const char *data = &buf[0];
                const char* start = data;
                const char* end = start;
                const char* terminus = start + buf.size();
                bool finished = false;
                while (!finished)
                {
                    while( end != terminus && (*end)!=attDelim && (*end)!=lineDelim )
                    {
                        ++end;
                    }
                    writer.writeValue(start, end);
                    if(end == terminus || (*end) == lineDelim )
                    {
                        writer.endLine();
                        ++nLines;
                        if (nLines > outputChunkSize)
                        {
                            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered a string with more lines than the chunk size; bailing";
                        }
                        if(end == terminus)
                        {
                            finished = true;
                        }
                    }
                    if (end != terminus)
                    {
                        start = end+1;
                        end   = end+1;
                    }
                }
            }
            ++(*inputIterator);
        }
        return writer.finalize();
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalUberLoad, "uber_load", "PhysicalUberLoad");


} // end namespace scidb
