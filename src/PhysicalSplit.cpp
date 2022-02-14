/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2020 Paradigm4 Inc.
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

#include <cstring>
#include <limits>
#include <sstream>

#include <boost/unordered_map.hpp>
#include <query/PhysicalOperator.h>
#include <util/Platform.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <array/SinglePassArray.h>
#include <system/Sysinfo.h>
#include "SplitSettings.h"

namespace scidb
{

using namespace std;

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
        _inputFile = ::fopen(filePath.c_str(), "r");
        // todo: investigate whether using a larger FILE* buffer will help, e..g
        // char sbuffer[128*1024];  // but aligned
        // setbuffer(_inputFile, sbuffer, sizeof(sbuffer));

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
                nread = getdelim(&line, &linesize, (int)_delimiter, _inputFile);
            }
            free(line);
        }
        _dataSize = ::fread_unlocked(&_buffer[0], 1, _bufferSize, _inputFile);
        if (_dataSize != _bufferSize)
        {
            _endOfFile= true;
            ::fclose(_inputFile);
            _inputFile =0;
        }
        _dataStartPos = &_buffer[0];
    }

    ~FileSplitter()
    {
        if(_inputFile!=0)
        {
            ::fclose(_inputFile);
        }
    }

    /**
     * Get a pointer to the next block of data which shall contain no more than numLinesPerBlock delimiter characters,
     * may contain less if we are at the end of the file. Also advances the position and reads more data from the file
     * if needed.
     * @param[out] numCharacters the size of returned data block, 0 if there is no more data
     * @return pointer to the data, not valid if numCharacters is 0
     */
    char* getBlock(size_t& numCharactersRslt)
    {
        size_t lineCounter = _linesPerBlock;
        char* ch = _dataStartPos;
        size_t numChar = 0;
        while (1)
        {
            while (numChar < _dataSize && lineCounter != 0)
            {
                char * p = static_cast<char*>(
                               memchr(ch, _delimiter, _dataSize-numChar));
                if(p) {
                    lineCounter --;             // *p is the delimiter
                    assert(p >= ch);
                    numChar += p+1 - ch ;
                    ch = p+1 ;
                } else {                        // end reached, p is 0
                    ch = _dataStartPos + _dataSize;     // end position
                    numChar = _dataSize;          // end count
                }
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
        _dataSize = _dataSize - numChar; // _dataSize wrapped?
        numCharactersRslt = numChar;
        return res;
    }

private:
    char* eatMoreData()
    {
        char *bufStart = &_buffer[0];
        if (_dataStartPos != bufStart)   //we have a block of data at the end of the buffer, move it to the beginning, then read more
        {
            // this memmove is very expensive. it can be as much as 50% of the operator time,
            // depending on numCharacters vs _buffersize
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

        // note is inefficient to use buffered io (f{open,read} etc) when reading full blocks
        // it is faster to do ::read() of 4K - 128K at a time (or more).
        size_t bytesRead = ::fread_unlocked( newDataStart, 1, remainderSize, _inputFile);
        if(bytesRead != remainderSize)
        {
            _endOfFile = true;
            ::fclose(_inputFile);
            _inputFile =0;
        }
        _dataStartPos = bufStart;
        _dataSize = _dataSize + bytesRead;
        return newDataStart;
    }
};


class EmptySinglePass : public SinglePassArray
{
private:
    typedef SinglePassArray super;
    MemChunk _dummy;

public:
    EmptySinglePass(ArrayDesc const& schema):
        super(schema),
        _dummy(SCIDB_CODE_LOC)
    {
        super::setEnforceHorizontalIteration(true);
    }
    virtual ~EmptySinglePass()
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

class FileSplitArray : public SinglePassArray
{
private:
    typedef SinglePassArray super;
    size_t _rowIndex;
    Address _chunkAddress;
    MemChunk _chunk;
    weak_ptr<Query> _query;
    FileSplitter _splitter;
    char*  _buffer;
    size_t _bufferSize;
    char   _delimiter;

public:
    FileSplitArray(ArrayDesc const& schema, shared_ptr<Query>& query, shared_ptr<SplitSettings> const& settings):
        super(schema),
        _rowIndex(0),
        _chunkAddress(0, Coordinates(2,0)),
        _chunk(SCIDB_CODE_LOC),
        _query(query),
        _splitter(settings->getInputFilePath(),
                  settings->getLinesPerChunk(),
                  settings->getBufferSize(),
                  settings->getDelimiter(),
                  settings->getHeader()),
        _delimiter(settings->getDelimiter())
    {
        super::setEnforceHorizontalIteration(true);
        _chunkAddress.coords[0] = settings->getParseInstance();
    }

    virtual ~FileSplitArray()
    {}

    size_t getCurrentRowIndex() const
    {
        return _rowIndex;
    }

    bool moveNext(size_t rowIndex)
    {
        _buffer = _splitter.getBlock(_bufferSize);
        if(_bufferSize > 0)
        {
            ++_rowIndex;
            return true;
        }
        else
        {
            return false;
        }
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        _chunkAddress.coords[1] = _rowIndex  -1;
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        _chunk.initialize(this, &super::getArrayDesc(), _chunkAddress, CompressorType::NONE);
        shared_ptr<ChunkIterator> chunkIt = _chunk.getIterator(query, ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
        Value v;
        if(_buffer[_bufferSize-1] == _delimiter) //add the null-termination character; replace the last delimiter character if one is present
        {
            v.setSize<Value::IGNORE_DATA>(_bufferSize);
            char *d = (char*) v.data();
            memcpy(d, _buffer, _bufferSize);
            d[_bufferSize-1] = 0;
        }
        else
        {
            v.setSize<Value::IGNORE_DATA>(_bufferSize+1);
            char *d = (char*) v.data();
            memcpy(d, _buffer, _bufferSize);
            d[_bufferSize] = 0;
        }
        chunkIt->writeItem(v);
        chunkIt->flush();
        return _chunk;
    }
};

class PhysicalSplit : public PhysicalOperator
{
public:
    PhysicalSplit(std::string const& logicalName,
                     std::string const& physicalName,
                     Parameters const& parameters,
                     ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    shared_ptr< Array> execute(std::vector<shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        shared_ptr<SplitSettings> settings (new SplitSettings (_parameters, false, query));
        shared_ptr<Array> result;
        ArrayDesc denseSchema(_schema);
        denseSchema.setAttributes(denseSchema.getDataAttributes());
        if( static_cast<int64_t> (query->getInstanceID()) == settings->getParseInstance())
        {
            result = shared_ptr<FileSplitArray>(new FileSplitArray(denseSchema, query, settings));
        }
        else
        {
            result = std::shared_ptr<EmptySinglePass>(new EmptySinglePass(denseSchema));
        }
        result = redistributeToRandomAccess(result,
                                            createDistribution(dtHashPartitioned),
                                            ArrayResPtr(),
                                            query,
                                            shared_from_this());
        result = std::shared_ptr<Array>(new NonEmptyableArray(result));
        return result;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalSplit, "split", "PhysicalSplit");

} // end namespace scidb
