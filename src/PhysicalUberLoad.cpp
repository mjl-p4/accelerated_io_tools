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
#include <log4cxx/logger.h>
#include <util/Network.h>

#include <boost/algorithm/string.hpp>

#include "UberLoadSettings.h"

using std::make_shared;

using boost::algorithm::is_from_range;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("alt.uber_load"));

using namespace scidb;


class BinaryFileSplitter
{
private:
    size_t const _fileBlockSize;
    size_t const _chunkOverheadSize;
    vector<char> _buffer;
    bool         _endOfFile;
    char*        _bufPointer;
    uint32_t*    _sizePointer;
    FILE*        _inputFile;

public:
    BinaryFileSplitter(string const& filePath,
                       size_t bufferSize,
                       int64_t header,
                       char lineDelimiter):
        _fileBlockSize(bufferSize),
        _chunkOverheadSize(  sizeof(ConstRLEPayload::Header) +
                             2 * sizeof(ConstRLEPayload::Segment) +
                             sizeof(varpart_offset_t) + 5),
        _endOfFile(false),
        _inputFile(0)
    {
        try
        {
            _buffer.resize(_chunkOverheadSize + _fileBlockSize);
        }
        catch(...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter() cannot allocate memory";
        }
        _bufPointer = &(_buffer[0]);
        ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) _bufPointer;
        hdr->_magic = RLE_PAYLOAD_MAGIC;
        hdr->_nSegs = 1;
        hdr->_elemSize = 0;
        hdr->_dataSize = _fileBlockSize + 5 + sizeof(varpart_offset_t);
        hdr->_varOffs = sizeof(varpart_offset_t);
        hdr->_isBoolean = 0;
        ConstRLEPayload::Segment* seg = (ConstRLEPayload::Segment*) (hdr+1);
        *seg =  ConstRLEPayload::Segment(0,0,false,false);
        ++seg;
        *seg =  ConstRLEPayload::Segment(1,0,false,false);
        varpart_offset_t* vp =  (varpart_offset_t*) (seg+1);
        *vp = 0;
        uint8_t* sizeFlag = (uint8_t*) (vp+1);
        *sizeFlag =0;
        _sizePointer = (uint32_t*) (sizeFlag + 1);
        *_sizePointer = (uint32_t) _fileBlockSize;
        _bufPointer = (char*) (_sizePointer+1);
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
        size_t numBytes = fread(_bufPointer, 1, _fileBlockSize, _inputFile);
        if(numBytes != _fileBlockSize)
        {
            _endOfFile = true;
            fclose(_inputFile);
            _inputFile = 0;
            if(numBytes == 0)
            {
                return false;
            }
            *_sizePointer = (uint32_t) numBytes;
        }
        return true;
    }

    char const* grabChunk(size_t &totalSize)
    {
        totalSize = _chunkOverheadSize + _fileBlockSize;
        return &_buffer[0];
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
        size_t bufSize;
        char const* buf = _splitter.grabChunk(bufSize);
        _chunk.allocate(bufSize);
        memcpy(_chunk.getData(),buf, bufSize);
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

    shared_ptr<Array> makeSupplement(shared_ptr<Array>& afterSplit, shared_ptr<Query>& query, shared_ptr<UberLoadSettings>& settings, vector<Coordinate>& lastBlocks)
    {
        char const lineDelim = settings->getLineDelimiter();
        shared_ptr<Array> supplement(new MemArray(getSplitSchema(), query));
        shared_ptr<ConstArrayIterator> srcArrayIter = afterSplit->getConstIterator(0);
        shared_ptr<ArrayIterator> dstArrayIter = supplement->getIterator(0);
        shared_ptr<ChunkIterator> dstChunkIter;
        while(!srcArrayIter->end())
        {
           Coordinates supplementCoords = srcArrayIter->getPosition();
           Coordinate iid   = supplementCoords[0];
           Coordinate block = supplementCoords[1];
           if(lastBlocks[iid] < block)
           {
               lastBlocks[iid] = block;
           }
           if(supplementCoords[1] != 0)
           {
               ConstChunk const& ch = srcArrayIter->getChunk();
               shared_ptr<ConstChunkIterator> srcChunkIter = ch.getConstIterator();
               Value const& v= srcChunkIter->getItem();
               supplementCoords[1]--;
               char const* start = static_cast<char const*>(v.data());
               char const* lim = start + v.size();
               char const* end = start;
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

    void exchangeLastBlocks(vector<Coordinate> &myLastBlocks, shared_ptr<Query>& query)
    {
        InstanceID const myId = query->getInstanceID();
        size_t const numInstances = query->getInstancesCount();
        size_t const vectorSize = numInstances *  sizeof(Coordinate);
        shared_ptr<SharedBuffer> buf(new MemoryBuffer( &(myLastBlocks[0]), vectorSize));
        for(InstanceID i = 0; i<numInstances; ++i)
        {
            if (i == myId)
            {
                continue;
            }
            BufSend(i, buf, query);
        }
        for(InstanceID i = 0; i<numInstances; ++i)
        {
            if (i == myId)
            {
                continue;
            }
            buf = BufReceive(i, query);
            vector<Coordinate> otherLastBlocks(numInstances);
            memcpy(&otherLastBlocks[0], buf->getData(), vectorSize);
            for(size_t j =0; j<numInstances; ++j)
            {
                if(otherLastBlocks[j] > myLastBlocks[j])
                {
                    myLastBlocks[j] = otherLastBlocks[j];
                }
            }
        }
        for(InstanceID i = 0; i<numInstances; ++i)
        {
            LOG4CXX_DEBUG(logger, "Last blocks instance "<<i<<" max "<<myLastBlocks[i]);
        }
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
        vector<Coordinate> lastBlocks(query->getInstancesCount(), -1);
        shared_ptr<Array> supplement = makeSupplement(splitData, query, settings, lastBlocks);
        exchangeLastBlocks(lastBlocks, query);
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
            bool const lastBlock = (lastBlocks[ pos[0] ] == pos[1]);
            shared_ptr<ConstChunkIterator> inputChunkIterator = inputIterator->getChunk().getConstIterator();
            if(inputChunkIterator->end()) //just 1 value in chunk
            {
                ++(*inputIterator);
                continue;
            }
            size_t nLines =0;
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
            LOG4CXX_DEBUG(logger, "Pos "<<CoordsToStr(pos) <<" lb "<< lastBlock << " s "<< buf.size());
            if(lastBlock && buf.size() <= 1)
            {
                ++(*inputIterator);
                continue;
            }
            const char *data = &buf[0];
            const char* start = data;
            const char* end = start;
            const char* terminus = start + buf.size();
            bool finished = false;
            writer.newChunk(pos, query);
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
                    if(end == terminus || (lastBlock && end == terminus-1))
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
            ++(*inputIterator);
        }
        return writer.finalize();
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalUberLoad, "uber_load", "PhysicalUberLoad");


} // end namespace scidb
