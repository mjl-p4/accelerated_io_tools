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

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>

#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/Sysinfo.h>

#ifdef USE_ARROW
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/io_util.h>
#endif

#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>
#include <query/PhysicalOperator.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <util/Platform.h>
#include <network/Network.h>
#include <array/SinglePassArray.h>
#include <array/SynchableArray.h>
#include <array/PinBuffer.h>

#include "UnparseTemplateParser.h"

#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>

#include "AioSaveSettings.h"


#ifdef USE_ARROW
#define THROW_NOT_OK(s)                                                 \
    {                                                                   \
        arrow::Status _s = (s);                                         \
        if (!_s.ok())                                                   \
        {                                                               \
            throw USER_EXCEPTION(                                       \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION)      \
                    << _s.ToString().c_str();                           \
        }                                                               \
    }

#define THROW_NOT_OK_FILE(s)                                            \
    {                                                                   \
        arrow::Status _s = (s);                                         \
        if (!_s.ok())                                                   \
        {                                                               \
            throw USER_EXCEPTION(                                       \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)       \
                    << _s.ToString().c_str() << (int)_s.code();         \
        }                                                               \
    }
#endif


namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.alt_save"));

using namespace scidb;

static void EXCEPTION_ASSERT(bool cond)
{
    if (! cond)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
    }
}

#ifdef USE_ARROW
std::shared_ptr<arrow::Schema> attributes2ArrowSchema(ArrayDesc const &arrayDesc,
                                                      bool attsOnly)
{
    Attributes const &attrs = arrayDesc.getAttributes(true);
    Dimensions const &dims = arrayDesc.getDimensions();

    size_t nAttrs = attrs.size();
    size_t nDims = dims.size();

    std::vector<std::shared_ptr<arrow::Field>> arrowFields(
        nAttrs + (attsOnly ? 0 : nDims));
    size_t i = 0;
    for (const auto& attr : attrs)
    {
        auto type = attr.getType();
        auto typeEnum = typeId2TypeEnum(type, true);
        std::shared_ptr<arrow::DataType> arrowType;

        switch (typeEnum)
        {
        case TE_BINARY:
        {
            arrowType = arrow::binary();
            break;
        }
        case TE_BOOL:
        {
            arrowType = arrow::boolean();
            break;
        }
        case TE_CHAR:
        {
            arrowType = arrow::utf8();
            break;
        }
        case TE_DATETIME:
        {
            arrowType = arrow::timestamp(arrow::TimeUnit::SECOND);
            break;
        }
        case TE_DOUBLE:
        {
            arrowType = arrow::float64();
            break;
        }
        case TE_FLOAT:
        {
            arrowType = arrow::float32();
            break;
        }
        case TE_INT8:
        {
            arrowType = arrow::int8();
            break;
        }
        case TE_INT16:
        {
            arrowType = arrow::int16();
            break;
        }
        case TE_INT32:
        {
            arrowType = arrow::int32();
            break;
        }
        case TE_INT64:
        {
            arrowType = arrow::int64();
            break;
        }
        case TE_UINT8:
        {
            arrowType = arrow::uint8();
            break;
        }
        case TE_UINT16:
        {
            arrowType = arrow::uint16();
            break;
        }
        case TE_UINT32:
        {
            arrowType = arrow::uint32();
            break;
        }
        case TE_UINT64:
        {
            arrowType = arrow::uint64();
            break;
        }
        case TE_STRING:
        {
            arrowType = arrow::utf8();
            break;
        }
        default:
        {
            ostringstream error;
            error << "Type " << type << " not supported in arrow format";
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
        }
        }

        arrowFields[i] = arrow::field(attr.getName(), arrowType);
        i++;
    }
    if (!attsOnly)
    {
        for (size_t i = 0; i < nDims; ++i)
        {
            arrowFields[nAttrs + i] = arrow::field(dims[i].getBaseName(), arrow::int64());
        }
    }

    return arrow::schema(arrowFields);
}
#endif

ArrayDesc const addDimensionsToArrayDesc(ArrayDesc const& arrayDesc,
                                         bool attsOnly,
                                         size_t nAttrs)
{
    ArrayDesc arrayDescWithDim(arrayDesc);

    Dimensions const &dims = arrayDesc.getDimensions();
    const size_t nDims = dims.size();

    for (size_t i = 0; i < nDims; ++i)
    {
        arrayDescWithDim.addAttribute(
            AttributeDesc(dims[i].getBaseName() + "val",
                          TID_INT64,
                          0,
                          CompressorType::NONE));
    }
    return arrayDescWithDim;
}

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
    static const size_t s_startingSize = 8*1024*1024 + 512;

    MemChunkBuilder():
        _allocSize(s_startingSize),
        _chunk(SCIDB_CODE_LOC)
    {
        _chunk.allocate(_allocSize, AllocType::chunk, SCIDB_CODE_LOC);
        _chunkStartPointer = (char*) _chunk.getWriteData();
        RLEPayload::PayloadHeader* hdr = (RLEPayload::PayloadHeader*) _chunkStartPointer;
        hdr->_magic = RLE_PAYLOAD_MAGIC;
        hdr->_nSegs = 1;
        hdr->_elemSize = 0;
        hdr->_dataSize = 0;
        _dataSizePointer = &(hdr->_dataSize);
        hdr->_varOffs = sizeof(varpart_offset_t);
        hdr->_isBoolean = 0;
        PayloadSegment* seg = (PayloadSegment*) (hdr+1);
        *seg =  PayloadSegment(0,0,false,false);
        ++seg;
        *seg =  PayloadSegment(1,0,false,false);
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

    inline void addData(char const* data, size_t const size)
    {
        if( getTotalSize() + size > _allocSize)
        {
            size_t const mySize = getTotalSize();
            while (mySize + size > _allocSize)
            {
                _allocSize = _allocSize * 2;
            }
            vector<char> buf(_allocSize);
            memcpy(&(buf[0]), _chunk.getWriteData(), mySize);
            _chunk.allocate(_allocSize,
                            AllocType::chunk, SCIDB_CODE_LOC);
            _chunkStartPointer = (char*) _chunk.getWriteData();
            memcpy(_chunkStartPointer, &(buf[0]), mySize);
            _dataStartPointer = _chunkStartPointer + AioSaveSettings::chunkDataOffset();
            _sizePointer = (uint32_t*) (_chunkStartPointer + AioSaveSettings::chunkSizeOffset());
            _writePointer = _chunkStartPointer + mySize;
            RLEPayload::PayloadHeader* hdr = (RLEPayload::PayloadHeader*) _chunkStartPointer;
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
        const auto& inputSchemaAttrs = input->getArrayDesc().getAttributes(true);
        for (const auto& attr : inputSchemaAttrs)
        {
            _inputArrayIters[attr.getId()] = _input->getConstIterator(attr);
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

    void advanceChunkIters()
    {
        if (_end)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal error: iterating past end of cursor";
        }
        if (_inputChunkIters[0] == 0) // 1st time!
        {
            for(size_t i = 0; i < _nAttrs; ++i)
            {
                _inputChunkIters[i] = _inputArrayIters[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS);
            }
        }
        else if (!_inputChunkIters[0]->end()) // not the first time!
        {
            for(size_t i = 0; i < _nAttrs; ++i)
            {
                ++(*_inputChunkIters[i]);
            }
        }
        while(_inputChunkIters[0]->end())
        {
            for(size_t i =0; i < _nAttrs; ++i)
            {
                ++(*_inputArrayIters[i]);
            }
            if(_inputArrayIters[0]->end())
            {
                _end = true;
                return;
            }
            for(size_t i =0; i < _nAttrs; ++i)
            {
                _inputChunkIters[i] = _inputArrayIters[i]->getChunk().getConstIterator(ConstChunkIterator::IGNORE_OVERLAPS);
            }
        }
    }

    void advance()
    {
        if (_end)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal error: iterating past end of cursor";
        }
        advanceChunkIters();
        if (_end)
        {
            return;
        }
        for(size_t i = 0; i < _nAttrs; ++i)
        {
            _currentCell[i] = &(_inputChunkIters[i]->getItem());
        }
    }

    vector <Value const *> const& getCell()
    {
        return _currentCell;
    }

    shared_ptr<ConstChunkIterator> getChunkIter(size_t i)
    {
        return _inputChunkIters[i];
    }

    Coordinates const& getPosition()
    {
        return _inputChunkIters[0]->getPosition();
    }
};

class BinaryChunkPopulator
{
private:
    bool             _attsOnly;
    size_t const     _nAttrs;
    size_t const     _nDims;
    ExchangeTemplate _templ;
    size_t const     _nColumns;
    vector< Value >  _cnvValues;
    vector< char >   _padBuffer;

    static inline size_t skip_bytes(ExchangeTemplate::Column const& c)
    {
        SCIDB_ASSERT(c.skip);
        return (c.fixedSize ? c.fixedSize : sizeof(uint32_t)) + c.nullable;
    }

public:
    BinaryChunkPopulator(ArrayDesc const& inputArrayDesc,
                         AioSaveSettings const& settings):
        _attsOnly(settings.isAttsOnly()),
        _nAttrs(inputArrayDesc.getAttributes(true).size()),
        _nDims(inputArrayDesc.getDimensions().size()),
        _templ(TemplateParser::parse(
                   _attsOnly ? inputArrayDesc : addDimensionsToArrayDesc(inputArrayDesc, _attsOnly, _nAttrs),
                   settings.getBinaryFormatString(),
                   false)),
        _nColumns(_templ.columns.size()),
        _cnvValues(_nAttrs),
        _padBuffer(sizeof(uint64_t) + 1, '\0')
    {
        for (size_t c = 0, i = 0; c < _nAttrs; ++c)
        {
          ExchangeTemplate::Column const& column = _templ.columns[c];
          if (column.skip)
          {
              // Prepare to write (enough) padding.
              size_t pad = skip_bytes(column);
              if (pad > _padBuffer.size())
              {
                  _padBuffer.resize(pad, '\0');
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
    }

    ~BinaryChunkPopulator()
    {}

    void populateChunk(MemChunkBuilder& builder, ArrayCursor& cursor, size_t const bytesPerChunk, int64_t const cellsPerChunk)
    {
        int64_t nCells = 0;
        while( !cursor.end() && ((cellsPerChunk<=0 && builder.getTotalSize() < bytesPerChunk) || (cellsPerChunk > 0 && nCells < cellsPerChunk)))
        {
            vector <Value const *> const& cell = cursor.getCell();
            Coordinates const &coords = cursor.getPosition();

            for (size_t c = 0, i = 0; c < _nColumns; ++c)
            {
                if (c < _nAttrs)
                {
                    ExchangeTemplate::Column const& column = _templ.columns[c];
                    if (column.skip)
                    {
                        size_t pad = skip_bytes(column);
                        builder.addData(&(_padBuffer[0]), _padBuffer.size());
                    }
                    else
                    {
                        Value const* v = cell[i];
                        if (column.nullable)
                        {
                            int8_t missingReason = (int8_t)v->getMissingReason();
                            builder.addData( (char*) (&missingReason), sizeof(missingReason));
                        }
                        if (v->isNull())
                        {
                            if (!column.nullable)
                            {
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ASSIGNING_NULL_TO_NON_NULLABLE);
                            }
                            // for varying size type write 4-bytes counter
                            size_t size = column.fixedSize ? column.fixedSize : sizeof(uint32_t);
                            builder.addData( &(_padBuffer[0]), size);
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
                                builder.addData( (char*) (&size), sizeof(size));
                                builder.addData( (char*) v->data(), size);
                            }
                            else
                            {
                                if (size > column.fixedSize)
                                {
                                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION) << size << column.fixedSize;
                                }
                                builder.addData( (char*) v->data(), size);
                                if (size < column.fixedSize)
                                {
                                    size_t padSize = column.fixedSize - size;
                                    assert(padSize <= _padBuffer.size());
                                    builder.addData(&(_padBuffer[0]), padSize);
                                }
                            }
                        }
                        ++i;
                    }
                }
                else
                {
                    if (!_attsOnly)
                    {
                        builder.addData((char*)(&coords[c - _nAttrs]), 8);
                    }
                }
            }
            cursor.advance();
            ++nCells;
        }
    }
};

#ifdef USE_ARROW
class ArrowChunkPopulator
{

private:
    const Attributes&                                 _attrs;
    const size_t                                      _nDims;
    const bool                                        _attsOnly;
    const std::shared_ptr<arrow::Schema>              _arrowSchema;

    std::vector<TypeEnum>                             _inputTypes;
    std::vector<size_t>                               _inputSizes;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> _arrowBuilders;
    std::vector<std::shared_ptr<arrow::Array>>        _arrowArrays;
    arrow::MemoryPool*                                _arrowPool =
      arrow::default_memory_pool();
    std::vector<std::vector<int64_t>>                 _dimsValues;

public:
    ArrowChunkPopulator(ArrayDesc const& inputArrayDesc,
                        AioSaveSettings const& settings):
        _attrs(inputArrayDesc.getAttributes(true)),
        _nDims(inputArrayDesc.getDimensions().size()),
        _attsOnly(settings.isAttsOnly()),
        _arrowSchema(attributes2ArrowSchema(inputArrayDesc, _attsOnly))
    {
        const size_t nAttrs = _attrs.size();

        _inputTypes.resize(nAttrs);
        _inputSizes.resize(nAttrs);
        _arrowBuilders.resize(nAttrs + (_attsOnly ? 0 : _nDims));
        _arrowArrays.resize(nAttrs + (_attsOnly ? 0 : _nDims));

        // Create Arrow Builders
        size_t i = 0;
        for (const auto& attr : _attrs)
        {
            _inputTypes[i] = typeId2TypeEnum(attr.getType(), true);
            _inputSizes[i] = attr.getSize() +
                (attr.isNullable() ? 1 : 0);

            THROW_NOT_OK(
                arrow::MakeBuilder(
                    _arrowPool,
                    _arrowSchema->field(i)->type(),
                    &_arrowBuilders[i]));
            i++;
        }
        if (!_attsOnly)
        {
            for(size_t i = nAttrs; i < nAttrs + _nDims; ++i)
            {
                THROW_NOT_OK(
                    arrow::MakeBuilder(
                        _arrowPool,
                        _arrowSchema->field(i)->type(),
                        &_arrowBuilders[i]));
            }

            // Setup coordinates buffers
            _dimsValues = std::vector<std::vector<int64_t>>(_nDims);
        }
    }

    ~ArrowChunkPopulator()
    {}

    void populateChunk(MemChunkBuilder& builder,
                       ArrayCursor& cursor,
                       size_t const bytesPerChunk,
                       int16_t const cellsPerChunk)
    {
        THROW_NOT_OK(
            populateChunkStatus(builder, cursor, bytesPerChunk, cellsPerChunk));
    }


private:
    arrow::Status populateChunkStatus(MemChunkBuilder& builder,
                                      ArrayCursor& cursor,
                                      size_t const bytesPerChunk,
                                      int16_t const cellsPerChunk)
    {
        // Basic setup
        const size_t nAttrs = _attrs.size();

        // Append to Arrow Builders
        int64_t nCells = 0;
        size_t bytesCount = 0;

        while (!cursor.end() &&
               ((cellsPerChunk <= 0 && bytesCount < bytesPerChunk) ||
                (cellsPerChunk > 0 && nCells < cellsPerChunk)))
        {
            for (size_t i = 0; i < nAttrs; ++i)
            {
                shared_ptr<ConstChunkIterator> citer = cursor.getChunkIter(i);

                // Reset coordinate buffers
                if (!_attsOnly && i == 0)
                {
                    for (size_t j = 0; j < _nDims; ++j)
                    {
                        _dimsValues[j].clear();
                    }
                }

                switch (_inputTypes[i])
                {
                case TE_BINARY:
                {
                    while (!citer->end())
                    {
                        Value const& value = citer->getItem();
                        if(value.isNull())
                        {
                            ARROW_RETURN_NOT_OK(
                                static_cast<arrow::BinaryBuilder*>(
                                    _arrowBuilders[i].get())->AppendNull());
                        }
                        else
                        {
                            ARROW_RETURN_NOT_OK(
                                static_cast<arrow::BinaryBuilder*>(
                                    _arrowBuilders[i].get())->Append(
                                        reinterpret_cast<const char*>(
                                            value.data()),
                                        value.size()));
                        }
                        bytesCount += _inputSizes[i] + value.size();

                        // Store coordinates in the buffer
                        if (!_attsOnly && i == 0 )
                        {
                            Coordinates const &coords = citer->getPosition();
                            for (size_t j = 0; j < _nDims; ++j)
                            {
                                _dimsValues[j].push_back(coords[j]);
                                bytesCount += 8;
                            }
                        }

                        ++(*citer);
                    }
                    break;
                }
                case TE_STRING:
                {
                    vector<string> values;
                    vector<uint8_t> is_valid;

                    while (!citer->end())
                    {
                        Value const& value = citer->getItem();
                        if(value.isNull())
                        {
                            values.push_back("");
                            is_valid.push_back(0);
                        }
                        else
                        {
                            values.push_back(value.getString());
                            is_valid.push_back(1);
                        }
                        bytesCount += _inputSizes[i] + value.size();

                        // Store coordinates in the buffer
                        if (!_attsOnly && i == 0 )
                        {
                            Coordinates const &coords = citer->getPosition();
                            for (size_t j = 0; j < _nDims; ++j)
                            {
                                _dimsValues[j].push_back(coords[j]);
                                bytesCount += 8;
                            }
                        }

                        ++(*citer);
                    }

                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::StringBuilder*>(
                            _arrowBuilders[i].get())->AppendValues(values, is_valid.data()));
                    break;
                }
                case TE_CHAR:
                {
                    vector<string> values;
                    vector<uint8_t> is_valid;

                    while (!citer->end())
                    {
                        Value const& value = citer->getItem();
                        if(value.isNull())
                        {
                            values.push_back("");
                            is_valid.push_back(0);
                        }
                        else
                        {
                            values.push_back(string(1, value.getChar()));
                            is_valid.push_back(1);
                        }
                        bytesCount += _inputSizes[i] + value.size();

                        // Store coordinates in the buffer
                        if (!_attsOnly && i == 0 )
                        {
                            Coordinates const &coords = citer->getPosition();
                            for (size_t j = 0; j < _nDims; ++j)
                            {
                                _dimsValues[j].push_back(coords[j]);
                                bytesCount += 8;
                            }
                        }

                        ++(*citer);
                    }

                    ARROW_RETURN_NOT_OK(
                        static_cast<arrow::StringBuilder*>(
                            _arrowBuilders[i].get())->AppendValues(values, is_valid.data()));
                    break;
                }
                case TE_BOOL:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<bool,arrow::BooleanBuilder>(
                            citer,
                            &Value::getBool,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_DATETIME:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int64_t,arrow::Date64Builder>(
                            citer,
                            &Value::getDateTime,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_DOUBLE:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<double,arrow::DoubleBuilder>(
                            citer,
                            &Value::getDouble,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_FLOAT:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<float,arrow::FloatBuilder>(
                            citer,
                            &Value::getFloat,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_INT8:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int8_t,arrow::Int8Builder>(
                            citer,
                            &Value::getInt8,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_INT16:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int16_t,arrow::Int16Builder>(
                            citer,
                            &Value::getInt16,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_INT32:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int32_t,arrow::Int32Builder>(
                            citer,
                            &Value::getInt32,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_INT64:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<int64_t,arrow::Int64Builder>(
                            citer,
                            &Value::getInt64,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_UINT8:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint8_t,arrow::UInt8Builder>(
                            citer,
                            &Value::getUint8,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_UINT16:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint16_t,arrow::UInt16Builder>(
                            citer,
                            &Value::getUint16,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_UINT32:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint32_t,arrow::UInt32Builder>(
                            citer,
                            &Value::getUint32,
                            i,
                            bytesCount)));
                    break;
                }
                case TE_UINT64:
                {
                    ARROW_RETURN_NOT_OK((
                        populateCell<uint64_t,arrow::UInt64Builder>(
                            citer,
                            &Value::getUint64,
                            i,
                            bytesCount)));
                    break;
                }
                default:
                {
                    ostringstream error;
                    error << "Type "
                          << _inputTypes[i]
                          << " not supported in arrow format";
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
                                         SCIDB_LE_ILLEGAL_OPERATION) << error.str();
                }
                }

                if (i == 0)
                {
                    ++nCells;

                    // Store coordinates in Arrow arrays
                    if (!_attsOnly)
                    {
                        for (size_t j = 0; j < _nDims; ++j)
                        {
                            ARROW_RETURN_NOT_OK(
                                static_cast<arrow::Int64Builder*>(
                                    _arrowBuilders[nAttrs + j].get()
                                    )->AppendValues(_dimsValues[j]));
                        }
                    }
                }
            }

            cursor.advanceChunkIters();
        }

        // Finalize Arrow Builders and populate Arrow Arrays (resets builders)
        for (size_t i = 0; i < nAttrs + (_attsOnly ? 0 : _nDims); ++i)
        {
            ARROW_RETURN_NOT_OK(
                _arrowBuilders[i]->Finish(&_arrowArrays[i])); // Resets builder
        }

        // Create Arrow Record Batch
        std::shared_ptr<arrow::RecordBatch> arrowBatch;
        arrowBatch = arrow::RecordBatch::Make(_arrowSchema, _arrowArrays[0]->length(), _arrowArrays);
        ARROW_RETURN_NOT_OK(arrowBatch->Validate());

        // Stream Arrow Record Batch to Arrow Buffer using Arrow
        // Record Batch Writer and Arrow Buffer Output Stream
        std::shared_ptr<arrow::io::BufferOutputStream> arrowStream;
        ARROW_ASSIGN_OR_RAISE(
            arrowStream,
            arrow::io::BufferOutputStream::Create(bytesCount * 2, _arrowPool));

        std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
        ARROW_RETURN_NOT_OK(
            arrow::ipc::RecordBatchStreamWriter::Open(
                &*arrowStream, _arrowSchema, &arrowWriter));
        // Arrow >= 0.17.0
        // ARROW_ASSIGN_OR_RAISE(
        //     arrowWriter,
        //     arrow::ipc::NewStreamWriter(&*arrowStream, _arrowSchema));

        ARROW_RETURN_NOT_OK(arrowWriter->WriteRecordBatch(*arrowBatch));
        ARROW_RETURN_NOT_OK(arrowWriter->Close());

        std::shared_ptr<arrow::Buffer> arrowBuffer;
        ARROW_ASSIGN_OR_RAISE(arrowBuffer, arrowStream->Finish());

        LOG4CXX_DEBUG(logger,
                      "ALT_SAVE>> ArrowChunkPopulator::populateChunkStatus bytesCount x2: "
                      << bytesCount * 2 << " arrowBuffer::size: " << arrowBuffer->size())

        // Copy data to Mem Chunk Builder
        builder.addData(reinterpret_cast<const char*>(arrowBuffer->data()),
                        arrowBuffer->size());

        return arrow::Status::OK();
    }

    template <typename SciDBType,
              typename ArrowBuilder,
              typename ValueFunc> inline
    arrow::Status populateCell(shared_ptr<ConstChunkIterator> citer,
                               ValueFunc valueGetter,
                               const size_t i,
                               size_t &bytesCount)
    {
        vector<SciDBType> values;
        vector<bool> is_valid;

        while (!citer->end())
        {
            Value const& value = citer->getItem();
            if(value.isNull())
            {
                values.push_back(0);
                is_valid.push_back(false);
            }
            else
            {
                values.push_back((value.*valueGetter)());
                is_valid.push_back(true);
            }
            bytesCount += _inputSizes[i];

            // Store coordinates in the buffer
            if (!_attsOnly && i == 0 )
            {
                Coordinates const &coords = citer->getPosition();
                for (size_t j = 0; j < _nDims; ++j)
                {
                    _dimsValues[j].push_back(coords[j]);
                    bytesCount += 8;
                }
            }

            ++(*citer);
        }

        return static_cast<ArrowBuilder*>(
            _arrowBuilders[i].get())->AppendValues(values, is_valid);
    }
};
#endif

class TextChunkPopulator
{

private:
    enum AttType
    {
        OTHER   =0,
        STRING =1,
        FLOAT  =2,
        DOUBLE =3,
        BOOL   =4,
        UINT8  =5,
        INT8    =6
    };

    char const              _attDelim;
    char const              _lineDelim;
    bool const              _printCoords;
    bool const              _quoteStrings;
    vector<AttType>         _attTypes;
    vector<FunctionPointer> _converters;
    Value                   _stringBuf;
    AioSaveSettings const   _settings;
    string                  _nanRepresentation;

public:
    TextChunkPopulator(ArrayDesc const& inputArrayDesc,
                       AioSaveSettings const& settings):
       _attDelim(settings.getAttributeDelimiter()),
       _lineDelim(settings.getLineDelimiter()),
       _printCoords(settings.printCoordinates()),
       _quoteStrings(settings.quoteStrings()),
       _attTypes(inputArrayDesc.getAttributes(true).size(), OTHER),
       _converters(inputArrayDesc.getAttributes(true).size(), 0),
       _settings(settings),
       _nanRepresentation("nan")
    {
        Attributes const& inputAttrs = inputArrayDesc.getAttributes(true);
        size_t i = 0;
        for (const auto& attr : inputAttrs)
        {
            if (attr.getType() == TID_STRING)
            {
                _attTypes[attr.getId()] = STRING;
            }
            else if(attr.getType() == TID_BOOL)
            {
                _attTypes[attr.getId()] = BOOL;
            }
            else if(attr.getType() == TID_DOUBLE)
            {
                _attTypes[attr.getId()] = DOUBLE;
            }
            else if(attr.getType() == TID_FLOAT)
            {
                _attTypes[attr.getId()] = FLOAT;
            }
            else if(attr.getType() == TID_UINT8)
            {
                _attTypes[attr.getId()] = UINT8;
            }
            else if(attr.getType() == TID_INT8)
            {
                _attTypes[attr.getId()] = INT8;
            }
            else
            {
                _converters[i] = FunctionLibrary::getInstance()->findConverter(
                    attr.getType(),
                    TID_STRING,
                    false);
            }
            i++;
        }
    }

    ~TextChunkPopulator()
    {}

    void populateChunk(MemChunkBuilder& builder, ArrayCursor& cursor, size_t const bytesPerChunk, int64_t const cellsPerChunk)
    {
        int64_t nCells = 0;
        ostringstream outputBuf;
        outputBuf.precision(_settings.getPrecision());
        size_t bufferSize = AioSaveSettings::chunkDataOffset();
        while( !cursor.end() && ( (cellsPerChunk<=0 && bufferSize < bytesPerChunk) ||  (cellsPerChunk > 0 && nCells < cellsPerChunk) ))
        {
            if(_printCoords)
            {
                Coordinates const& pos = cursor.getPosition();
                for(size_t i =0, n=pos.size(); i<n; ++i)
                {
                    if(i)
                    {
                        outputBuf<<_attDelim;
                    }
                    outputBuf<<pos[i];
                }
            }
            vector <Value const *> const& cell = cursor.getCell();
            for (size_t i = 0; i < cursor.nAttrs(); ++i)
            {
                Value const* v = cell[i];
                if (i || _printCoords)
                {
                    outputBuf<<_attDelim;
                }
                if(v->isNull())
                {
                    _settings.printNull(outputBuf, v->getMissingReason());
                }
                else
                {
                    switch(_attTypes[i])
                    {
                    case STRING:
                        if(_quoteStrings)
                        {
                            char const* s = v->getString();
                            outputBuf << '\'';
                            while (char c = *s++)
                            {
                                if (c == '\'')
                                {
                                    outputBuf << '\\' << c;
                                }
                                else if (c == '\\')
                                {
                                    outputBuf << "\\\\";
                                }
                                else
                                {
                                    outputBuf << c;
                                }
                            }
                            outputBuf << '\'';
                        }
                        else
                        {
                            outputBuf<<v->getString();
                        }
                        break;
                    case BOOL:
                        if(v->getBool())
                        {
                            outputBuf<<"true";
                        }
                        else
                        {
                            outputBuf<<"false";
                        }
                        break;
                    case DOUBLE:
                        {
                            double nbr =v->getDouble();
                            if(std::isnan(nbr))
                            {
                                outputBuf<<_nanRepresentation;
                            }
                            else
                            {
                                outputBuf<<nbr;
                            }
                        }
                        break;
                    case FLOAT:
                        {
                            float fnbr =v->getFloat();
                            if(std::isnan(fnbr))
                            {
                                outputBuf<<_nanRepresentation;
                            }
                            else
                            {
                                outputBuf<<fnbr;
                            }
                        }
                        break;
                    case UINT8:
                        {
                            uint8_t nbr =v->getUint8();
                            outputBuf<<(int16_t) nbr;
                        }
                        break;
                    case INT8:
                        {
                            int8_t nbr =v->getUint8();
                            outputBuf<<(int16_t) nbr;
                        }
                        break;
                    case OTHER:
                        (*_converters[i])(&v, &_stringBuf, NULL);
                        outputBuf<<_stringBuf.getString();
                    }
                }
            }
            outputBuf<<_lineDelim;
            cursor.advance();
            bufferSize =   AioSaveSettings::chunkDataOffset() + outputBuf.tellp() + 1;
            ++nCells;
        }
        string s = outputBuf.str();
        builder.addData(s.c_str(), s.size());
    }
};

template <class ChunkPopulator>
class ConversionArray: public SinglePassArray
{
private:
    typedef SinglePassArray super;
    size_t                                   _rowIndex;
    Address                                  _chunkAddress;
    ArrayCursor                              _inputCursor;
    MemChunkBuilder                          _chunkBuilder;
    weak_ptr<Query>                          _query;
    size_t const                             _bytesPerChunk;
    int64_t const                            _cellsPerChunk;
    ChunkPopulator                           _populator;
    map<InstanceID, string>                  _instanceMap;
    map<InstanceID, string>::const_iterator  _mapIter;

public:
    ConversionArray(ArrayDesc const& schema,
                    shared_ptr<Array>& inputArray,
                    shared_ptr<Query>& query,
                    AioSaveSettings const& settings):
        super(schema),
        _rowIndex(0),
        _chunkAddress(0, Coordinates(3,0)),
        _inputCursor(inputArray),
        _query(query),
        _bytesPerChunk(settings.getBufferSize()),
        _cellsPerChunk(settings.getCellsPerChunk()),
        _populator(inputArray->getArrayDesc(), settings),
        _instanceMap(settings.getInstanceMap()),
        _mapIter(_instanceMap.begin())
    {

        InstanceID const myInstanceID = query->getInstanceID();
        _chunkAddress.coords[2] = myInstanceID;
        //offset the first instance I send data to - for a more even distribution
        InstanceID i =0;
        while (i < myInstanceID)
        {
            ++_mapIter;
            if(_mapIter == _instanceMap.end())
            {
                _mapIter = _instanceMap.begin();
            }
            ++i;
        }
    }

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
        _populator.populateChunk(_chunkBuilder, _inputCursor, _bytesPerChunk, _cellsPerChunk);
        ++_rowIndex;
        return true;
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        _chunkAddress.coords[0] = _rowIndex  -1;
        _chunkAddress.coords[1] = _mapIter->first;
        ++_mapIter;
        if(_mapIter == _instanceMap.end())
        {
            _mapIter = _instanceMap.begin();
        }
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        MemChunk& ch = _chunkBuilder.getChunk();
        ch.initialize(this, &super::getArrayDesc(), _chunkAddress, CompressorType::NONE);
        return ch;
    }
};

typedef ConversionArray <BinaryChunkPopulator> BinaryConvertedArray;
#ifdef USE_ARROW
typedef ConversionArray <ArrowChunkPopulator>  ArrowConvertedArray;
#endif
typedef ConversionArray <TextChunkPopulator>   TextConvertedArray;

uint64_t saveToDisk(shared_ptr<Array> const& array,
                    string file,
                    std::shared_ptr<Query> const& query,
                    bool const append,
                    AioSaveSettings const& settings,
                    ArrayDesc const& inputSchema)
{
    ArrayDesc const& desc = array->getArrayDesc();
    const size_t N_ATTRS = desc.getAttributes(true).size();
    EXCEPTION_ASSERT(N_ATTRS==1);
    FILE* f;
    LOG4CXX_DEBUG(logger, "ALT_SAVE>> opening file")
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
        f = ::fopen(file.c_str(), settings.isBinaryFormat() ? append ? "ab" : "wb" : append ? "a" : "w");
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
    LOG4CXX_DEBUG(logger, "ALT_SAVE>> starting write")
    size_t bytesWritten = 0;
    try
    {
        if(settings.printHeader())
        {
            ostringstream header;
            if(settings.printCoordinates())
            {
                for(size_t i =0; i<inputSchema.getDimensions().size(); ++i)
                {
                    if(i)
                    {
                        header<<settings.getAttributeDelimiter();
                    }
                    header<<inputSchema.getDimensions()[i].getBaseName();
                }
            }
            size_t i = 0;
            for (const auto& attr : inputSchema.getAttributes(true))
            {
                if(i || settings.printCoordinates())
                {
                    header<<settings.getAttributeDelimiter();
                }
                header<<attr.getName();
                i++;
            }
            header<<settings.getLineDelimiter();
            ::fprintf(f, "%s", header.str().c_str());
        }
        shared_ptr<ConstArrayIterator> arrayIter = array->getConstIterator(inputSchema.getAttributes(true).firstDataAttribute());
        for (size_t n = 0; !arrayIter->end(); n++)
        {
            ConstChunk const& ch = arrayIter->getChunk();
            PinBuffer scope(ch);
            uint32_t* sizePointer = (uint32_t*) (((char*)ch.getConstData()) + AioSaveSettings::chunkSizeOffset());
            uint32_t size = *sizePointer;
            bytesWritten += size;
            if (bytesWritten >= settings.getResultSizeLimit())
            {
                int err = errno ? errno : EIO;
                LOG4CXX_INFO(logger, "Attempting to write " << bytesWritten << " bytes to " << file << "  when limit is " << settings.getResultSizeLimit());
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR) << "Exceeding specified result size limit of" << settings.getResultSizeLimit();
            }
            char* data = ((char*)ch.getConstData() + AioSaveSettings::chunkDataOffset());
            if (::fwrite(data, 1, size, f) != size)
            {
                int err = errno ? errno : EIO;
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR) << ::strerror(err) << err;
            }
            ++(*arrayIter);
        }
    }
    catch (scidb::Exception& e)
    {
        if (f == stdout || f == stderr)
        {
            ::fflush(f);
        }
        else
        {
            ::fclose(f);
        }
        e.raise();
    }
    LOG4CXX_DEBUG(logger, "ALT_SAVE>> wrote "<< bytesWritten<< " bytes, closing")
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
    LOG4CXX_DEBUG(logger, "ALT_SAVE>> closed")
    return 0;
}

#ifdef USE_ARROW
arrow::Status saveToDiskArrow(shared_ptr<Array> const& array,
                              string fileName,
                              std::shared_ptr<Query> const& query,
                              bool const append,
                              AioSaveSettings const& settings,
                              ArrayDesc const& inputSchema)
{
    EXCEPTION_ASSERT(array->getArrayDesc().getAttributes(true).size()==1);

    LOG4CXX_DEBUG(logger, "ALT_SAVE>> opening file");
    std::shared_ptr<arrow::io::OutputStream> arrowStream;
    if (fileName == "console" || fileName == "stdout")
    {
        arrowStream.reset(new arrow::io::StdoutStream());
    }
    else if (fileName == "stderr")
    {
        arrowStream.reset(new arrow::io::StderrStream());
    }
    else
    {
        auto arrowResult = arrow::io::FileOutputStream::Open(fileName, append);
        if (!arrowResult.ok())
        {
            auto arrowStatus = arrowResult.status();
            auto str = arrowStatus.ToString().c_str();
            int code = (int)arrowStatus.code();
            LOG4CXX_DEBUG(logger,
                          "Attempted to open output file '"
                          << fileName << "' failed: " << str << " (" << code << ")");
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_CANT_OPEN_FILE)
                 << fileName << str << code;
        }
        arrowStream = arrowResult.ValueOrDie();
        // TODO: is file lock necessary?
    }

    LOG4CXX_DEBUG(logger, "ALT_SAVE>> starting write");
    std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
    std::shared_ptr<arrow::RecordBatch> arrowBatch;
    std::shared_ptr<arrow::RecordBatchReader> arrowReader;
    size_t bytesWritten = 0;
    try
    {
        std::shared_ptr<arrow::Schema> arrowSchema = attributes2ArrowSchema(
            inputSchema, settings.isAttsOnly());
        ARROW_RETURN_NOT_OK(
            arrow::ipc::RecordBatchStreamWriter::Open(
                arrowStream.get(), arrowSchema, &arrowWriter));
        // Arrow >= 0.17.0
        // ARROW_ASSIGN_OR_RAISE(
        //     arrowWriter,
        //     arrow::ipc::NewStreamWriter(arrowStream.get(), arrowSchema));

        shared_ptr<ConstArrayIterator> arrayIter =
            array->getConstIterator(inputSchema.getAttributes(true).firstDataAttribute());
        for (size_t n = 0; !arrayIter->end(); n++)
        {
            ConstChunk const& chunk = arrayIter->getChunk();
            PinBuffer scope(chunk);
            uint32_t* sizePointer = (uint32_t*) (((char*)chunk.getConstData()) +
                                                 AioSaveSettings::chunkSizeOffset());
            uint32_t size = *sizePointer;
            bytesWritten += size;
            if (bytesWritten >= settings.getResultSizeLimit())
            {
                LOG4CXX_INFO(logger,
                             "Attempted to write " << bytesWritten
                             << " bytes to '" << fileName
                             << "' which is over specified limit.");
                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
                    << "Exceeding specified result size limit of"
                    << settings.getResultSizeLimit();
            }
            char* data = ((char*)chunk.getConstData() + AioSaveSettings::chunkDataOffset());

            arrow::io::BufferReader arrowBufferReader(
                reinterpret_cast<const uint8_t*>(data), size); // zero copy

            // Read Record Batch using Stream Reader
            ARROW_RETURN_NOT_OK(
                arrow::ipc::RecordBatchStreamReader::Open(
                    &arrowBufferReader, &arrowReader));
            // Arrow >= 0.17.0
            // ARROW_ASSIGN_OR_RAISE(
            //     arrowReader,
            //     arrow::ipc::RecordBatchStreamReader::Open(&arrowBufferReader));
            ARROW_RETURN_NOT_OK(arrowReader->ReadNext(&arrowBatch));

            // Write Record Batch to stream
            ARROW_RETURN_NOT_OK(
                arrowWriter->WriteRecordBatch(*arrowBatch));

            ++(*arrayIter);
        }
    }
    catch (scidb::Exception& e)
    {
        if (arrowWriter != nullptr)
        {
            arrowWriter->Close();
        }
        if (fileName == "console" || fileName == "stdout" || fileName == "stderr")
        {
            arrowStream->Flush();
        }
        else
        {
            arrowStream->Close();
        }
        e.raise();
    }

    LOG4CXX_DEBUG(logger, "ALT_SAVE>> wrote "<< bytesWritten<< " bytes, closing");
    ARROW_RETURN_NOT_OK(arrowWriter->Close());
    if (fileName == "console" || fileName == "stdout" || fileName == "stderr")
    {
        ARROW_RETURN_NOT_OK(arrowStream->Flush());
    }
    else
    {
        ARROW_RETURN_NOT_OK(arrowStream->Close());
    }
    LOG4CXX_DEBUG(logger, "ALT_SAVE>> closed");
    return arrow::Status::OK();
}
#endif


class PhysicalAioSave : public PhysicalOperator
{
public:
    PhysicalAioSave(std::string const& logicalName,
                    std::string const& physicalName,
                    Parameters const& parameters,
                    ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    bool isSingleChunk(ArrayDesc const& schema)
    {
        for(size_t i =0; i<schema.getDimensions().size(); ++i)
        {
            DimensionDesc const& d = schema.getDimensions()[i];
            if(((uint64_t) d.getChunkInterval()) != d.getLength())
            {
                return false;
            }
        }
        return true;
    }

    bool haveChunk(shared_ptr<Array>& input, ArrayDesc const& schema)
    {
        shared_ptr<ConstArrayIterator> iter = input->getConstIterator(schema.getAttributes(true).firstDataAttribute());
        return !(iter->end());
    }

    /**
     * If all nodes call this with true - return true.
     * Otherwise, return false.
     */
    bool agreeOnBoolean(bool value, shared_ptr<Query>& query)
    {
        std::shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(bool)));
        InstanceID myId = query->getInstanceID();
        *((bool*) buf->getWriteData()) = value;
        for(InstanceID i=0; i<query->getInstancesCount(); i++)
        {
            if(i != myId)
            {
                BufSend(i, buf, query);
            }
        }
        for(InstanceID i=0; i<query->getInstancesCount(); i++)
        {
            if(i != myId)
            {
                buf = BufReceive(i,query);
                bool otherInstanceVal = *((bool*) buf->getConstData());
                value = value && otherInstanceVal;
            }
        }
        return value;
    }

    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        AioSaveSettings settings (_parameters, _kwParameters, false, query);
        shared_ptr<Array>& input = inputArrays[0];
        ArrayDesc const& inputSchema = input->getArrayDesc();
        bool singleChunk = isSingleChunk(inputSchema);
        shared_ptr< Array> outArray;
        shared_ptr<Array> result;
        ArrayDesc denseSchema(_schema);
        denseSchema.setAttributes(denseSchema.getDataAttributes());
        if(settings.isBinaryFormat())
        {
            outArray.reset(new BinaryConvertedArray(denseSchema, input, query, settings));
        }
#ifdef USE_ARROW
        else if(settings.isArrowFormat())
        {
            outArray.reset(new ArrowConvertedArray(denseSchema, input, query, settings));
        }
#endif
        else
        {
            outArray.reset(new TextConvertedArray(denseSchema, input, query, settings));
        }
        InstanceID const myInstanceID = query->getInstanceID();
        map<InstanceID, string>::const_iterator iter = settings.getInstanceMap().find(myInstanceID);
        bool thisInstanceSavesData = (iter != settings.getInstanceMap().end());
        if(singleChunk && agreeOnBoolean((thisInstanceSavesData == haveChunk(input, inputSchema)), query))
        {
            LOG4CXX_DEBUG(logger, "ALT_SAVE>> single-chunk path")
            if(thisInstanceSavesData)
            {
                string const& path = iter->second;
#ifdef USE_ARROW
                if (settings.isArrowFormat())
                {
                    THROW_NOT_OK_FILE(
                        saveToDiskArrow(
                            outArray, path, query, false, settings, inputSchema));
                }
                else
#endif
                {
                    saveToDisk(
                        outArray, path, query, false, settings, inputSchema);
                }
            }
            result = shared_ptr<Array>(new MemArray(denseSchema, query));
            return shared_ptr<Array>(new NonEmptyableArray(result));
        }
        shared_ptr<Array> outArrayRedist;
        LOG4CXX_DEBUG(logger, "ALT_SAVE>> Starting SG")
        outArrayRedist = pullRedistribute(outArray,
                                          createDistribution(dtByCol),
                                          ArrayResPtr(),
                                          query,
                                          shared_from_this());
        bool const wasConverted = (outArrayRedist != outArray) ;
        if (thisInstanceSavesData)
        {
            string const& path = iter->second;
#ifdef USE_ARROW
            if (settings.isArrowFormat())
            {
                THROW_NOT_OK_FILE(
                    saveToDiskArrow(
                        outArrayRedist, path, query, false, settings, inputSchema));
            }
            else
#endif
            {
                saveToDisk(
                    outArrayRedist, path, query, false, settings, inputSchema);
            }
        }
        if (wasConverted)
        {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(outArrayRedist.get());
            syncArray->sync();
        }
        result = shared_ptr<Array>(new MemArray(denseSchema, query));
        return shared_ptr<Array>(new NonEmptyableArray(result));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalAioSave, "aio_save", "PhysicalAioSave");

} // end namespace scidb
