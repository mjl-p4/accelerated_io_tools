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
#include "SplitSettings.h"

namespace scidb
{
using namespace boost;
using namespace scidb;


/**
 * A wrapper around an open file (or pipe) that may iterate over the data once and split it into blocks, each
 * block containing a number of lines. Returns one block at a time.
 */
class FileSplitter
{
private:
    size_t const _linesPerBlock;
    size_t       _bufferSize;
    vector<char> _buffer;
    char*        _dataStartPos;
    size_t       _dataSize;
    bool         _endOfFile;
    FILE*        _inputFile;
    char         _delimiter;

public:
    FileSplitter(string const& filePath,
                 size_t numLinesPerBlock,
                 size_t bufferSize,
                 char delimiter,
                 int64_t header):
        _linesPerBlock(numLinesPerBlock),
        _bufferSize(bufferSize),
        _buffer(0),
        _dataStartPos(0),
        _dataSize(0),
        _endOfFile(false),
        _inputFile(0),
        _delimiter(delimiter)
    {
        try
        {
            _buffer.resize(_bufferSize);
        }
        catch(...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter() cannot allocate memory";
        }
        _inputFile = fopen(filePath.c_str(), "r");
        if(header>0)
        {
            char *line = NULL;
            size_t linesize = 0;
            ssize_t nread = 0;
            for(int64_t j=0; j<header && nread>=0; ++j)
            {
                nread = getdelim(&line, &linesize, (int)_delimiter, _inputFile);
            }
            free(line);
        }
        if (_inputFile == NULL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter() cannot open file";
        }
        _dataSize = fread(&_buffer[0], 1, _bufferSize, _inputFile);
        if (_dataSize != _bufferSize)
        {
            _endOfFile= true;
            fclose(_inputFile);
            _inputFile =0;
        }
        _dataStartPos = &_buffer[0];
    }

    ~FileSplitter()
    {
        if(_inputFile!=0)
        {
            fclose(_inputFile);
        }
    }

    /**
     * Get a pointer to the next block of data which shall contain no more than numLinesPerBlock delimiter characters,
     * may contain less if we are at the end of the file. Also advances the position and reads more data from the file
     * if needed.
     * @param[out] numCharacters the size of returned data block, 0 if there is no more data
     * @return pointer to the data, not valid if numCharacters is 0
     */
    char* getBlock(size_t& numCharacters)
    {
        size_t lineCounter = _linesPerBlock;
        char* ch = _dataStartPos;
        numCharacters = 0;
        while (1)
        {
            while (numCharacters < _dataSize && lineCounter != 0)
            {
                if(*ch == _delimiter)
                {
                    lineCounter --;
                }
                ++ch;
                ++numCharacters;
            }
            if(lineCounter == 0 || _endOfFile)
            {
                break;
            }
            else
            {
                ch = eatMoreData(); //this call changes _dataStartPos and _dataSize
            }
        }
        char* res = _dataStartPos;
        _dataStartPos = ch;
        _dataSize = _dataSize - numCharacters;
        return res;
    }

private:
    char* eatMoreData()
    {
        char *bufStart = &_buffer[0];
        if (_dataStartPos != bufStart)   //we have a block of data at the end of the buffer, move it to the beginning, then read more
        {
            memmove(bufStart, _dataStartPos, _dataSize);
        }
        else
        {
            if(_dataSize != _bufferSize) //invariant check: entire buffer must be full; double the size of the buffer, then read more
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter()::eatMoreData internal error";
            }
            _bufferSize = _bufferSize * 2;
            try
            {
                _buffer.resize(_bufferSize);
            }
            catch(...)
            {
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitter()::eatMoreData cannot allocate memory";
            }
            bufStart = &_buffer[0];
        }
        char *newDataStart = bufStart + _dataSize;
        size_t remainderSize = _bufferSize - _dataSize;
        size_t bytesRead = fread( newDataStart, 1, remainderSize, _inputFile);
        if(bytesRead != remainderSize)
        {
            _endOfFile = true;
            fclose(_inputFile);
            _inputFile =0;
        }
        _dataStartPos = bufStart;
        _dataSize = _dataSize + bytesRead;
        return newDataStart;
    }
};

class FileSplitArrayIterator : public ConstArrayIterator
{
private:
    Array const* _parentArray;
    weak_ptr<Query> _query;
    FileSplitter _splitter;
    Address _chunkAddress;
    char   _delimiter;
    char*  _buffer;
    size_t _bufferSize;
    MemChunk _chunk;

public:
    FileSplitArrayIterator(Array const* parent, weak_ptr<Query> const& query, shared_ptr<SplitSettings> const& settings):
        _parentArray(parent),
        _query(query),
        _splitter(settings->getInputFilePath(),
                  settings->getLinesPerChunk(),
                  settings->getBufferSize(),
                  settings->getDelimiter(),
                  settings->getHeader()),
				  _chunkAddress(0, Coordinates(2,0)),
         _delimiter(settings->getDelimiter())
    {

    	_chunkAddress.coords[0] = settings->getParseInstance();
    	_buffer = _splitter.getBlock(_bufferSize);
    }

    virtual bool end()
    {
        return _bufferSize == 0;
    }

    virtual void operator ++()
    {
        if(end())
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitArrayIterator::++ attempt to iterate past end";
        }
        ++(_chunkAddress.coords[1]);
        _buffer = _splitter.getBlock(_bufferSize);
    }

    virtual Coordinates const& getPosition()
    {
        return _chunkAddress.coords;
    }

    virtual bool setPosition(Coordinates const& pos)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitArrayIterator::setPosition() not supported";
    }

    virtual void reset()
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitArrayIterator::reset() not supported";
    }

    virtual ConstChunk const& getChunk()
    {
        _chunk.initialize(_parentArray, &_parentArray->getArrayDesc(), _chunkAddress, 0);
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        shared_ptr<ChunkIterator> chunkIt = _chunk.getIterator(query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        Value v;
        if(_buffer[_bufferSize-1] == _delimiter) //add the null-termination character; replace the last delimiter character if one is present
        {
            v.setSize(_bufferSize);
            char *d = (char*) v.data();
            memcpy(d, _buffer, _bufferSize);
            d[_bufferSize-1] = 0;
        }
        else
        {
            v.setSize(_bufferSize+1);
            char *d = (char*) v.data();
            memcpy(d, _buffer, _bufferSize);
            d[_bufferSize] = 0;
        }
        chunkIt->writeItem(v);
        chunkIt->flush();
        return _chunk;
    }
};

class FileSplitArray : public Array
{
private:
    ArrayDesc _desc;
    shared_ptr<SplitSettings> _settings;

public:
    FileSplitArray(ArrayDesc const& schema, shared_ptr<Query> const& query, shared_ptr<SplitSettings> const& settings):
        _desc(schema),
        _settings(settings)
    {
        _query = query;
    }

    virtual ~FileSplitArray()
    {}

    virtual Access getSupportedAccess() const
    {
        return SINGLE_PASS;
    }

    ArrayDesc const& getArrayDesc() const
    {
        return _desc;
    }

    virtual shared_ptr<ConstArrayIterator> getConstIterator(AttributeID attr) const
    {
        if(attr != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "FileSplitArray::getConstIterator request for nonexistent attribute";
        }
        return shared_ptr<ConstArrayIterator> (new FileSplitArrayIterator(this, _query, _settings));
    }
};

//TODO: in 14.9 Igor introduced a new SinglePassArray which gives us much better form
//But we also wanna make this work for 14.8 for now
//class FileSplitArray : public SinglePassArray
//{
//private:
//    typedef SinglePassArray super;
//    size_t _rowIndex;
//    Address _chunkAddress;
//    MemChunk _chunk;
//    weak_ptr<Query> _query;
//    FileSplitter _splitter;
//    char*  _buffer;
//    size_t _bufferSize;
//
//public:
//    FileSplitArray(ArrayDesc const& schema, shared_ptr<Query>& query,
//                   string const& filePath, size_t numLinesPerBlock, size_t bufferSize, char delimiter):
//        super(schema),
//        _rowIndex(0),
//        _chunkAddress(0, Coordinates(1,-1)),
//        _query(query),
//        _splitter(filePath, numLinesPerBlock, bufferSize, delimiter)
//    {
//        super::setEnforceHorizontalIteration(true);
//    }
//
//    virtual ~FileSplitArray()
//    {}
//
//    size_t getCurrentRowIndex() const
//    {
//        return _rowIndex;
//    }
//
//    bool moveNext(size_t rowIndex)
//    {
//        _buffer = _splitter.getBlock(_bufferSize);
//        if(_bufferSize > 0)
//        {
//            ++_rowIndex;
//            ++(_chunkAddress.coords[0]);
//            return true;
//        }
//        else
//        {
//            return false;
//        }
//    }
//
//    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
//    {
//        _chunk.initialize(this, &super::getArrayDesc(), _chunkAddress, 0);
//        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
//        shared_ptr<ChunkIterator> chunkIt = _chunk.getIterator(query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
//        Value v;
//        v.setSize(_bufferSize+1);
//        char *d = (char*) v.data();
//        memcpy(d, _buffer, _bufferSize);
//        d[_bufferSize] = 0;
//        chunkIt->writeItem(v);
//        chunkIt->flush();
//        return _chunk;
//    }
//};

class PhysicalSplit : public PhysicalOperator
{
public:
    PhysicalSplit(std::string const& logicalName,
                     std::string const& physicalName,
                     Parameters const& parameters,
                     ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual ArrayDistribution getOutputDistribution(std::vector<ArrayDistribution> const&, std::vector<ArrayDesc> const&) const
    {
        //TODO: change me when input from different instances or parallel is allowed.
        //see PhysicalInput.cpp
        //TODO: after 14.9 make FileSplitArray a subclass of SinglePassArray. After that, the explicit redistribute is not necessary
        return ArrayDistribution(psHashPartitioned);
    }

    boost::shared_ptr< Array> execute(std::vector< boost::shared_ptr< Array> >& inputArrays, boost::shared_ptr<Query> query)
    {
        shared_ptr<SplitSettings> settings (new SplitSettings (_parameters, false, query));

        /*
          if (settings->getSourceInstanceId() != 0)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "PhysicalSplit::execute internal error";
        }
        */

        shared_ptr<Array> result;
        LOG4CXX_DEBUG(logger, "multiplepath:PHYSICAL "<< query->getInstanceID()  << " " << settings->getParseInstance());
        if(query->getInstanceID() == settings->getParseInstance())
        {
            result = shared_ptr<FileSplitArray>(new FileSplitArray(_schema, query, settings));
        }
        else
        {
            result = boost::shared_ptr<MemArray>(new MemArray(_schema,query));
        }
        //TODO: after 14.9 make FileSplitArray a subclass of SinglePassArray. After that, the explicit redistribute is not necessary
        result = redistribute(result, query, psHashPartitioned);
        return result;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalSplit, "multisplit", "PhysicalSplit");


} // end namespace scidb
