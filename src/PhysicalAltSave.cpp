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
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>

#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/Sysinfo.h>

#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/Operator.h>
#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>
#include <query/Operator.h>
#include <array/DBArray.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <util/Platform.h>

#include "UnparseTemplateParser.h"

#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>
#include "AltSaveSettings.h"

#ifdef CPP11
using std::make_shared;
#else
using boost::make_shared;
#endif

using std::make_shared;
using boost::algorithm::is_from_range;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.unparsephysical"));

using namespace scidb;

static void EXCEPTION_ASSERT(bool cond)
{
    if (! cond)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
    }
}

static size_t chunkDataOffset()
{
    return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 5);
}

static size_t chunkSizeOffset()
{
    return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 1);
}

class GrowingBuffer
{
private:
    vector<char> _data;
    size_t       _allocSize;
    char*        _writePointer;
    char*        _startPointer;

public:
    static const size_t startingSize = 20*1024*1024;

    GrowingBuffer():
        _data(startingSize),
        _allocSize(startingSize),
        _writePointer(&_data[0]),
        _startPointer(&_data[0])
    {}

    ~GrowingBuffer()
    {}

    inline char* getData() const
    {
        return _startPointer;
    }

    inline size_t getSize() const
    {
        return _writePointer - _startPointer;
    }

    inline void addData(char* data, size_t const size)
    {
        if( getSize() + size > _allocSize)
        {
            size_t const mySize = getSize();
            _allocSize = _allocSize * 2;
            _data.resize(_allocSize);
            _startPointer = &_data[0];
            _writePointer = _startPointer + mySize;
        }
        memcpy(_writePointer, data, size);
        _writePointer += size;
    }

    inline void clear()
    {
        _writePointer = _startPointer;
    }
};

class MemChunkBuilder
{
private:
    size_t      _allocSize;
    char*       _chunkStartPointer;
    char*       _dataStartPointer;
    char*       _writePointer;
    uint32_t*   _sizePointer;
    uint64_t*   _dataSizePointer;
    MemChunk    _chunk;

public:
    static size_t chunkDataOffset()
    {
        return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 5);
    }

    static size_t chunkSizeOffset()
    {
        return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 1);
    }

    static const size_t s_startingSize = 20*1024*1024;

    MemChunkBuilder():
        _allocSize(s_startingSize)
    {
        _chunk.allocate(_allocSize);
        _chunkStartPointer = (char*) _chunk.getData();
        ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) _chunkStartPointer;
        hdr->_magic = RLE_PAYLOAD_MAGIC;
        hdr->_nSegs = 1;
        hdr->_elemSize = 0;
        hdr->_dataSize = 0;
        _dataSizePointer = &(hdr->_dataSize);
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
        _dataStartPointer = (char*) (_sizePointer+1);
        _writePointer = _dataStartPointer;
    }

    ~MemChunkBuilder()
    {}

    inline size_t getTotalSize() const
    {
        return (_writePointer - _chunkStartPointer);
    }

    inline void addData(char* data, size_t const size)
    {
        if( getTotalSize() + size > _allocSize)
        {
            size_t const mySize = getTotalSize();
            _allocSize = _allocSize * 2;
            _chunk.allocate(_allocSize);
            _chunkStartPointer = (char*) _chunk.getData();
            _dataStartPointer = _chunkStartPointer + chunkDataOffset();
            _sizePointer = (uint32_t*) (_chunkStartPointer + chunkSizeOffset());
            _writePointer = _chunkStartPointer + mySize;
            ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) _chunkStartPointer;
            _dataSizePointer = &(hdr->_dataSize);
        }
        memcpy(_writePointer, data, size);
        _writePointer += size;
    }

    inline MemChunk& getChunk()
    {
        *_sizePointer = (_writePointer - _dataStartPointer);
        *_dataSizePointer = (_writePointer - _dataStartPointer) + 5 + sizeof(varpart_offset_t);
        return _chunk;
    }

    inline void reset()
    {
        _writePointer = _dataStartPointer;
    }
};

class SaveArrayBuilder
{
private:
    shared_ptr<MemArray>          _out;
    Coordinates                   _chunkPos;
    shared_ptr<Query> const&      _query;
    size_t const                  _chunkOverheadSize;
    shared_ptr<ArrayIterator>     _aiter;

public:
    SaveArrayBuilder (ArrayDesc const& schema,
                     std::shared_ptr<Query> const& query):
        _out(new MemArray(schema, query)),
        _chunkPos(2,0),
        _query(query),
        _chunkOverheadSize( sizeof(ConstRLEPayload::Header) +
                            2 * sizeof(ConstRLEPayload::Segment) +
                            sizeof(varpart_offset_t) + 5)
    {
        _chunkPos[0]  = query->getInstanceID();
        _chunkPos[1] = -1;
        _aiter = _out->getIterator(0);
    }

    void addValue(Value const& v0)
    {
        _chunkPos[1] += 1;
//        shared_ptr<ChunkIterator> citer = _aiter->newChunk(_chunkPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE);
//        citer ->setPosition(_chunkPos);
//        citer ->writeItem(v0);
//        citer ->flush();
        size_t const valSize = v0.size();
        Chunk& newChunk = _aiter->newChunk(_chunkPos);
        PinBuffer pinScope(newChunk);
        newChunk.allocate(_chunkOverheadSize + valSize);
        char* bufPointer = (char*) newChunk.getData();
        ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) bufPointer;
        hdr->_magic = RLE_PAYLOAD_MAGIC;
        hdr->_nSegs = 1;
        hdr->_elemSize = 0;
        hdr->_dataSize = valSize + 5 + sizeof(varpart_offset_t);
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
        uint32_t* sizePointer = (uint32_t*) (sizeFlag + 1);
        *sizePointer = (uint32_t) valSize;
        bufPointer = (char*) (sizePointer+1);
        memcpy(bufPointer, v0.data(), valSize);
    }

    void addBuffer(GrowingBuffer const& b)
    {
        _chunkPos[1] += 1;
       size_t const valSize = b.getSize();
       Chunk& newChunk = _aiter->newChunk(_chunkPos);
       PinBuffer pinScope(newChunk);
       newChunk.allocate(_chunkOverheadSize + valSize);
       char* bufPointer = (char*) newChunk.getData();
       ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) bufPointer;
       hdr->_magic = RLE_PAYLOAD_MAGIC;
       hdr->_nSegs = 1;
       hdr->_elemSize = 0;
       hdr->_dataSize = valSize + 5 + sizeof(varpart_offset_t);
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
       uint32_t* sizePointer = (uint32_t*) (sizeFlag + 1);
       *sizePointer = (uint32_t) valSize;
       bufPointer = (char*) (sizePointer+1);
       memcpy(bufPointer, b.getData(), valSize);
    }

    std::shared_ptr<Array> finalize()
    {
        return _out;
    }
};

class ArrayCursor
{
private:
    shared_ptr<Array> _input;
    size_t const _nAttrs;
    vector <Value const *> _currentCell;
    bool _end;
    vector<shared_ptr<ConstArrayIterator> > _inputArrayIters;
    vector<shared_ptr<ConstChunkIterator> > _inputChunkIters;

public:
    ArrayCursor (shared_ptr<Array> const& input):
        _input(input),
        _nAttrs(input->getArrayDesc().getAttributes(true).size()),
        _currentCell(_nAttrs, 0),
        _end(false),
        _inputArrayIters(_nAttrs, 0),
        _inputChunkIters(_nAttrs, 0)
    {
        for(size_t i =0; i<_nAttrs; ++i)
        {
            _inputArrayIters[i] = _input->getConstIterator(i);
        }
        if (_inputArrayIters[0]->end())
        {
            _end=true;
        }
        else
        {
            advance();
        }
    }

    bool end() const
    {
        return _end;
    }

    size_t nAttrs() const
    {
        return _nAttrs;
    }

    void advance()
    {
        if(_end)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal error: iterating past end of cursor";
        }
        if (_inputChunkIters[0] == 0) //1st time!
        {
            for(size_t i =0; i<_nAttrs; ++i)
            {
                _inputChunkIters[i] = _inputArrayIters[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS);
            }
        }
        else if (!_inputChunkIters[0]->end()) //not first time!
        {
            for(size_t i =0; i<_nAttrs; ++i)
            {
                ++(*_inputChunkIters[i]);
            }
        }
        while(_inputChunkIters[0]->end())
        {
            for(size_t i =0; i<_nAttrs; ++i)
            {
                ++(*_inputArrayIters[i]);
            }
            if(_inputArrayIters[0]->end())
            {
                _end = true;
                return;
            }
            for(size_t i =0; i<_nAttrs; ++i)
            {
                _inputChunkIters[i] = _inputArrayIters[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS);
            }
        }
        for(size_t i =0; i<_nAttrs; ++i)
        {
            _currentCell[i] = &(_inputChunkIters[i]->getItem());
        }
    }

    vector <Value const *> const& getCell()
    {
        return _currentCell;
    }
};

std::shared_ptr< Array> convertToTDV(shared_ptr<Array> const& inputArray,
                                     AltSaveSettings const& settings,
                                     SaveArrayBuilder& outputwriter)
{
    size_t linesperchunk = settings.getLinesPerChunk();
    ArrayDesc const& desc = inputArray->getArrayDesc();
    Attributes const& attrs = desc.getAttributes();
    AttributeDesc const* emptyAttr = desc.getEmptyBitmapAttribute();
    unsigned numAttrs = attrs.size() - (emptyAttr ? 1 : 0);
    if (numAttrs == 0)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal error: somehow ended up with a zero-attribute array";
    }
    vector<std::shared_ptr<ConstArrayIterator> > arrayIterators(numAttrs);
    vector<bool>                                 isString(numAttrs, false);
    vector<FunctionPointer>                      converters(numAttrs);
    for (unsigned i = 0, j = 0; i < attrs.size(); ++i)
    {
        if (emptyAttr && emptyAttr == &attrs[i])
            continue;
        arrayIterators[j] = inputArray->getConstIterator(i);
        if(attrs[i].getType() == TID_STRING)
        {
            isString[j]=true;
        }
        else
        {
            converters[j] = FunctionLibrary::getInstance()->findConverter(
                            attrs[i].getType(),
                            TID_STRING,
                            false);
        }
        ++j;
    }
    uint64_t lineCounter = 0;
    vector<std::shared_ptr<ConstChunkIterator> > chunkIterators(numAttrs);
    char const attDelim = settings.getAttributeDelimiter();
    char const lineDelim = settings.getLineDelimiter();
    ostringstream outputBuf;
    Value stringBuf;
    Value insertBuf;
    while (!arrayIterators[0]->end())
    {
        for (unsigned i = 0; i < numAttrs; ++i)
        {
            ConstChunk const& chunk = arrayIterators[i]->getChunk();
            chunkIterators[i] = chunk.getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS);
        }
        while (!chunkIterators[0]->end())
        {
            for (unsigned i = 0; i < numAttrs; ++i)
            {
                if (i)
                {
                    outputBuf<<attDelim;
                }
                Value const& v = chunkIterators[i]->getItem();
                if(v.isNull())
                {
                    //TODO: print nothing for now (the TSV way). In the future - add a null representation option to settings
                    outputBuf<<"\\N";
                }
                else
                {
                    if(isString[i])
                    {
                        outputBuf<<v.getString();
                    }
                    else
                    {
                        Value const* vptr = &v;
                        (*converters[i])(&vptr, &stringBuf, NULL);
                        outputBuf<<stringBuf.getString();
                    }
                }
                ++(*chunkIterators[i]);
            }
            // Another array cell for peace!
            outputBuf<<lineDelim;
            lineCounter++;
            if(lineCounter >= linesperchunk)
            {
                insertBuf.setString(outputBuf.str());
                outputwriter.addValue(insertBuf);
                lineCounter = 0;
                outputBuf.str("");
            }
        }
        // Bump the array iterators to get the next set of chunks.
        for (unsigned i = 0; i < numAttrs; ++i)
        {
            ++(*arrayIterators[i]);
        }
    }
    if(lineCounter)
    {
        insertBuf.setString(outputBuf.str());
        outputwriter.addValue(insertBuf);
    }
    std::shared_ptr<Array> outarray = outputwriter.finalize();
    return outarray;
}

static inline size_t skip_bytes(ExchangeTemplate::Column const& c)
{
    SCIDB_ASSERT(c.skip);
    return (c.fixedSize ? c.fixedSize : sizeof(uint32_t)) + c.nullable;
}

std::shared_ptr< Array>  convertToBinary(shared_ptr<Array> const& inputArray,
        AltSaveSettings const& settings,
        SaveArrayBuilder& outputWriter)
{
    ArrayCursor cursor(inputArray);
    ExchangeTemplate templ = TemplateParser::parse(inputArray->getArrayDesc(), settings.getBinaryFormatString(), false);
    const size_t N_ATTRS = cursor.nAttrs();
    const size_t N_COLUMNS = templ.columns.size();
    SCIDB_ASSERT(N_COLUMNS >= N_ATTRS); // One col per attr, plus "skip" columns
    vector< Value > cnvValues(N_ATTRS);
    vector< char > padBuffer(sizeof(uint64_t) + 1, '\0'); // Big enuf for all nullable built-ins
    GrowingBuffer chunkBuffer;
    size_t nMissingReasonOverflows = 0;
    for (size_t c = 0, i = 0; c < N_COLUMNS; ++c)
    {
        ExchangeTemplate::Column const& column = templ.columns[c];
        if (column.skip)
        {
            // Prepare to write (enough) padding.
            size_t pad = skip_bytes(column);
            if (pad > padBuffer.size())
            {
                padBuffer.resize(pad);
            }
        }
        else
        {
            // Prepare to write values.
            SCIDB_ASSERT(i < N_ATTRS);
            if (column.converter)
            {
                cnvValues[i] = Value(column.externalType);
            }
            ++i;            // next attribute
        }
    }
    uint64_t nCells = 0;    // aka number of tuples written
    while(!cursor.end())
    {
        vector <Value const *> cell = cursor.getCell();
        for (size_t c = 0, i = 0; c < N_COLUMNS; ++c)
        {
            ExchangeTemplate::Column const& column = templ.columns[c];
            if (column.skip)
            {
                size_t pad = skip_bytes(column);
                SCIDB_ASSERT(padBuffer.size() >= pad);
                chunkBuffer.addData(&(padBuffer[0]), padBuffer.size());
            }
            else
            {
                Value const* v = cell[i];
                if (column.nullable)
                {
                    if (v->getMissingReason() > 127)
                    {
                        LOG4CXX_WARN(logger, "Missing reason " << v->getMissingReason() << " cannot be stored in binary file");
                        nMissingReasonOverflows += 1;
                    }
                    int8_t missingReason = (int8_t)v->getMissingReason();
                    chunkBuffer.addData( (char*) (&missingReason), sizeof(missingReason));
                }
                if (v->isNull())
                {
                    if (!column.nullable)
                    {
                        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                    }
                    // for varying size type write 4-bytes counter
                    size_t size = column.fixedSize ? column.fixedSize : sizeof(uint32_t);
                    SCIDB_ASSERT(padBuffer.size() >= size);
                    chunkBuffer.addData( &(padBuffer[0]), size);
                }
                else
                {
                    if (column.converter)
                    {
                        column.converter(&v, &cnvValues[i], NULL);
                        v = &cnvValues[i];
                    }
                    if (v->size() > numeric_limits<uint32_t>::max())
                    {
                        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION) << v->size() << numeric_limits<uint32_t>::max();
                    }
                    uint32_t size = (uint32_t)v->size();
                    if (column.fixedSize == 0)
                    { // varying size type
                        chunkBuffer.addData( (char*) (&size), sizeof(size));
                        chunkBuffer.addData( (char*) v->data(), size);
                    }
                    else
                    {
                        if (size > column.fixedSize)
                        {
                            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION) << size << column.fixedSize;
                        }
                        chunkBuffer.addData( (char*) v->data(), size);
                        if (size < column.fixedSize)
                        {
                            size_t padSize = column.fixedSize - size;
                            assert(padSize <= padBuffer.size());
                            chunkBuffer.addData(&(padBuffer[0]), padSize);
                        }
                    }
                }
                ++i;
            }
        }
        ++nCells;
        if(nCells>settings.getLinesPerChunk())
        {
            outputWriter.addBuffer(chunkBuffer);
            chunkBuffer.clear();
            nCells = 0;
        }
        cursor.advance();
    }
    if(nCells)
    {
        outputWriter.addBuffer(chunkBuffer);
    }
    return outputWriter.finalize();
}

class BinaryConvertedArray : public SinglePassArray
{
private:
    typedef SinglePassArray super;
    size_t           _rowIndex;
    Address          _chunkAddress;
    ArrayCursor      _inputCursor;
    MemChunkBuilder  _chunkBuilder;
    weak_ptr<Query>  _query;
    ExchangeTemplate _templ;
    size_t const     _linesPerChunk;
    size_t const     _nAttrs;
    size_t const     _nColumns;
    vector< Value >  _cnvValues;
    vector< char >   _padBuffer;

public:
    BinaryConvertedArray(ArrayDesc const& schema,
                         shared_ptr<Array>& inputArray,
                         shared_ptr<Query>& query,
                         AltSaveSettings const& settings):
        super(schema),
        _rowIndex(0),
        _chunkAddress(0, Coordinates(2,0)),
        _inputCursor(inputArray),
        _query(query),
        _templ(TemplateParser::parse(inputArray->getArrayDesc(), settings.getBinaryFormatString(), false)),
        _linesPerChunk(settings.getLinesPerChunk()),
        _nAttrs(_inputCursor.nAttrs()),
        _nColumns(_templ.columns.size()),
        _cnvValues(_nAttrs),
        _padBuffer(sizeof(uint64_t) + 1, '\0')
    {
        _chunkAddress.coords[0] = query->getInstanceID();
    }

    virtual ~BinaryConvertedArray()
    {}

    size_t getCurrentRowIndex() const
    {
        return _rowIndex;
    }

    bool moveNext(size_t rowIndex)
    {
        if(_inputCursor.end())
        {
            return false;
        }
        _chunkBuilder.reset();
        size_t nCells = 0;
        for (size_t c = 0, i = 0; c < _nColumns; ++c)
        {
            ExchangeTemplate::Column const& column = _templ.columns[c];
            if (column.skip)
            {
                // Prepare to write (enough) padding.
                size_t pad = skip_bytes(column);
                if (pad > _padBuffer.size())
                {
                    _padBuffer.resize(pad);
                }
            }
            else
            {
                if (column.converter)
                {
                    _cnvValues[i] = Value(column.externalType);
                }
                ++i;            // next attribute
            }
        }
        while(nCells < _linesPerChunk && !_inputCursor.end())
        {
            vector <Value const *> cell = _inputCursor.getCell();
            for (size_t c = 0, i = 0; c < _nColumns; ++c)
            {
                ExchangeTemplate::Column const& column = _templ.columns[c];
                if (column.skip)
                {
                    size_t pad = skip_bytes(column);
                    _chunkBuilder.addData(&(_padBuffer[0]), _padBuffer.size());
                }
                else
                {
                    Value const* v = cell[i];
                    if (column.nullable)
                    {
                        if (v->getMissingReason() > 127)
                        {
                            LOG4CXX_WARN(logger, "Missing reason " << v->getMissingReason() << " cannot be stored in binary file");
                        }
                        int8_t missingReason = (int8_t)v->getMissingReason();
                        _chunkBuilder.addData( (char*) (&missingReason), sizeof(missingReason));
                    }
                    if (v->isNull())
                    {
                        if (!column.nullable)
                        {
                            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                        }
                        // for varying size type write 4-bytes counter
                        size_t size = column.fixedSize ? column.fixedSize : sizeof(uint32_t);
                        _chunkBuilder.addData( &(_padBuffer[0]), size);
                    }
                    else
                    {
                        if (column.converter)
                        {
                            column.converter(&v, &_cnvValues[i], NULL);
                            v = &_cnvValues[i];
                        }
                        if (v->size() > numeric_limits<uint32_t>::max())
                        {
                            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION) << v->size() << numeric_limits<uint32_t>::max();
                        }
                        uint32_t size = (uint32_t)v->size();
                        if (column.fixedSize == 0)
                        { // varying size type
                            _chunkBuilder.addData( (char*) (&size), sizeof(size));
                            _chunkBuilder.addData( (char*) v->data(), size);
                        }
                        else
                        {
                            if (size > column.fixedSize)
                            {
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION) << size << column.fixedSize;
                            }
                            _chunkBuilder.addData( (char*) v->data(), size);
                            if (size < column.fixedSize)
                            {
                                size_t padSize = column.fixedSize - size;
                                assert(padSize <= _padBuffer.size());
                                _chunkBuilder.addData(&(_padBuffer[0]), padSize);
                            }
                        }
                    }
                    ++i;
                }
            }
            _inputCursor.advance();
            ++nCells;
        }
        ++_rowIndex;
        return true;
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        _chunkAddress.coords[1] = _rowIndex  -1;
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        MemChunk& ch = _chunkBuilder.getChunk();
        ch.initialize(this, &super::getArrayDesc(), _chunkAddress, 0);
        return ch;
    }
};

//class TextConvertedArray : public SinglePassArray
//{
//private:
//    typedef SinglePassArray super;
//    size_t           _rowIndex;
//    Address          _chunkAddress;
//    ArrayCursor      _inputCursor;
//    MemChunkBuilder  _chunkBuilder;
//    weak_ptr<Query>  _query;
//    size_t const     _linesPerChunk;
//
//public:
//    TextConvertedArray(ArrayDesc const& schema,
//                         shared_ptr<Array>& inputArray,
//                         shared_ptr<Query>& query,
//                         AltSaveSettings const& settings):
//        super(schema),
//        _rowIndex(0),
//        _chunkAddress(0, Coordinates(2,0)),
//        _inputCursor(inputArray),
//        _query(query),
//        _linesPerChunk(settings.getLinesPerChunk())
//    {
//        _chunkAddress.coords[0] = query->getInstanceID();
//    }
//
//    virtual ~TextConvertedArray()
//    {}
//
//    size_t getCurrentRowIndex() const
//    {
//        return _rowIndex;
//    }
//
//    bool moveNext(size_t rowIndex)
//    {
//        if(_inputCursor.end())
//        {
//            return false;
//        }
//        _chunkBuilder.reset();
//        size_t nCells = 0;
//        for (size_t c = 0, i = 0; c < _nColumns; ++c)
//        {
//
//            ExchangeTemplate::Column const& column = _templ.columns[c];
//            if (column.skip)
//            {
//                // Prepare to write (enough) padding.
//                size_t pad = skip_bytes(column);
//                if (pad > _padBuffer.size())
//                {
//                    _padBuffer.resize(pad);
//                }
//            }
//            else
//            {
//                if (column.converter)
//                {
//                    _cnvValues[i] = Value(column.externalType);
//                }
//                ++i;            // next attribute
//            }
//        }
//        while(nCells < _linesPerChunk && !_inputCursor.end())
//        {
//            vector <Value const *> cell = _inputCursor.getCell();
//            for (size_t c = 0, i = 0; c < _nColumns; ++c)
//            {
//                ExchangeTemplate::Column const& column = _templ.columns[c];
//                if (column.skip)
//                {
//                    size_t pad = skip_bytes(column);
//                    _chunkBuilder.addData(&(_padBuffer[0]), _padBuffer.size());
//                }
//                else
//                {
//                    Value const* v = cell[i];
//                    if (column.nullable)
//                    {
//                        if (v->getMissingReason() > 127)
//                        {
//                            LOG4CXX_WARN(logger, "Missing reason " << v->getMissingReason() << " cannot be stored in binary file");
//                        }
//                        int8_t missingReason = (int8_t)v->getMissingReason();
//                        _chunkBuilder.addData( (char*) (&missingReason), sizeof(missingReason));
//                    }
//                    if (v->isNull())
//                    {
//                        if (!column.nullable)
//                        {
//                            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
//                        }
//                        // for varying size type write 4-bytes counter
//                        size_t size = column.fixedSize ? column.fixedSize : sizeof(uint32_t);
//                        _chunkBuilder.addData( &(_padBuffer[0]), size);
//                    }
//                    else
//                    {
//                        if (column.converter)
//                        {
//                            column.converter(&v, &_cnvValues[i], NULL);
//                            v = &_cnvValues[i];
//                        }
//                        if (v->size() > numeric_limits<uint32_t>::max())
//                        {
//                            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION) << v->size() << numeric_limits<uint32_t>::max();
//                        }
//                        uint32_t size = (uint32_t)v->size();
//                        if (column.fixedSize == 0)
//                        { // varying size type
//                            _chunkBuilder.addData( (char*) (&size), sizeof(size));
//                            _chunkBuilder.addData( (char*) v->data(), size);
//                        }
//                        else
//                        {
//                            if (size > column.fixedSize)
//                            {
//                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION) << size << column.fixedSize;
//                            }
//                            _chunkBuilder.addData( (char*) v->data(), size);
//                            if (size < column.fixedSize)
//                            {
//                                size_t padSize = column.fixedSize - size;
//                                assert(padSize <= _padBuffer.size());
//                                _chunkBuilder.addData(&(_padBuffer[0]), padSize);
//                            }
//                        }
//                    }
//                    ++i;
//                }
//            }
//            _inputCursor.advance();
//            ++nCells;
//        }
//        ++_rowIndex;
//        return true;
//    }
//
//    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
//    {
//        _chunkAddress.coords[1] = _rowIndex  -1;
//        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
//        MemChunk& ch = _chunkBuilder.getChunk();
//        ch.initialize(this, &super::getArrayDesc(), _chunkAddress, 0);
//        return ch;
//    }
//};



struct AwIoError
{
    AwIoError(int x) : error(x) {}
    int     error;
};

uint64_t saveToDisk(shared_ptr<Array> const& array,
                    string file,
                    std::shared_ptr<Query> const& query,
                    bool const isBinary,
                    bool const append)
{
    ArrayDesc const& desc = array->getArrayDesc();
    const size_t N_ATTRS = desc.getAttributes(true).size();
    EXCEPTION_ASSERT(N_ATTRS==1);
    FILE* f;
    if (file == "console" || file == "stdout")
    {
        f = stdout;
    }
    else if (file == "stderr")
    {
        f = stderr;
    }
    else
    {
        f = fopen(file.c_str(), isBinary ? append ? "ab" : "wb" : append ? "a" : "w");
        if (NULL == f)
        {
            int error = errno;
            LOG4CXX_DEBUG(logger, "Attempted to open output file '" << file << "' failed: " << ::strerror(error) << " (" << error);
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_CANT_OPEN_FILE) << file << ::strerror(error) << error;
        }
        struct flock flc;
        flc.l_type = F_WRLCK;
        flc.l_whence = SEEK_SET;
        flc.l_start = 0;
        flc.l_len = 1;
        int rc = fcntl(fileno(f), F_SETLK, &flc);
        if (rc == -1) {
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_CANT_LOCK_FILE) << file << ::strerror(errno) << errno;
        }
    }
    try
    {
        shared_ptr<ConstArrayIterator> arrayIter = array->getConstIterator(0);
        for (size_t n = 0; !arrayIter->end(); n++)
        {
            ConstChunk const& ch = arrayIter->getChunk();
            PinBuffer scope(ch);
            uint32_t* sizePointer = (uint32_t*) (((char*)ch.getData()) + chunkSizeOffset());
            uint32_t size = *sizePointer;
            if (!isBinary)
            {
                //strip away the terminating null character
                size = size-1;
            }
            char* data = ((char*)ch.getData() + chunkDataOffset());
            if (fwrite(data, 1, size, f) != size)
            {
                int err = errno ? errno : EIO;
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR) << ::strerror(err) << err;
            }
            ++(*arrayIter);
        }
    }
    catch (AwIoError& e)
    {
        if (f == stdout || f == stderr)
        {
            ::fflush(f);
        }
        else
        {
            ::fclose(f);
        }
        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR) << ::strerror(e.error) << e.error;
    }
    int rc(0);
    if (f == stdout || f == stderr)
    {
        rc = ::fflush(f);
    }
    else
    {
        rc = ::fclose(f);
    }
    if (rc != 0)
    {
        int err = errno ? errno : EIO;
        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR) << ::strerror(err) << err;
    }
    return 0;
}


class PhysicalAltSave : public PhysicalOperator
{
public:
    PhysicalAltSave(std::string const& logicalName,
                  std::string const& physicalName,
                  Parameters const& parameters,
                   ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

#ifdef CPP11
    virtual RedistributeContext getOutputDistribution(std::vector<RedistributeContext> const&, std::vector<ArrayDesc> const&) const
    {
        return RedistributeContext(psUndefined);
    }
#else
    virtual ArrayDistribution getOutputDistribution(std::vector<ArrayDistribution> const&, std::vector<ArrayDesc> const&) const
    {
        return ArrayDistribution(psUndefined);
    }
#endif

    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        AltSaveSettings settings (_parameters, false, query);
        SaveArrayBuilder writer(_schema, query);
        shared_ptr<Array>& input = inputArrays[0];
        shared_ptr< Array> outArray;
        if(settings.isBinaryFormat())
        {
            if(settings.materialize())
            {
                outArray = convertToBinary(input, settings, writer);
            }
            else
            {
                outArray.reset(new BinaryConvertedArray(_schema, input, query, settings));
            }
        }
        else
        {
            outArray = convertToTDV(input, settings, writer);
        }
        ArrayDesc const& inputArrayDesc = outArray->getArrayDesc();
        shared_ptr<Array> tmpRedistedInput;
        InstanceID const myInstanceID = query->getInstanceID();
        InstanceID const saveInstanceID = settings.getSaveInstanceId();
        const Attributes& attribs = inputArrayDesc.getAttributes();
        if(settings.push() == false)
        {
            tmpRedistedInput = pullRedistribute(outArray,
                                                query,
                                                psLocalInstance,
                                                saveInstanceID,
                                                std::shared_ptr<CoordinateTranslator>(),
                                                0,
                                                std::shared_ptr<PartitioningSchemaData>());
        }
        else
        {
            tmpRedistedInput = redistributeToRandomAccess(outArray,
                                                           query,
                                                           psLocalInstance,
                                                           saveInstanceID,
                                                           std::shared_ptr<CoordinateTranslator>(),
                                                           0,
                                                           std::shared_ptr<PartitioningSchemaData>());
        }
        bool const wasConverted = (tmpRedistedInput != outArray) ;
        if (saveInstanceID == myInstanceID)
        {
            saveToDisk(tmpRedistedInput, settings.getFilePath(), query, settings.isBinaryFormat(), false);
        }
        if (settings.push() == false && wasConverted)
        {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }
        return shared_ptr<Array>(new MemArray(_schema, query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalAltSave, "alt_save", "PhysicalAltSave");


} // end namespace scidb
