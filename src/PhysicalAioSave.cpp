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

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>

#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/Sysinfo.h>

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

#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/Operator.h>
#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>
#include <query/Operator.h>
//#include <array/DBArray.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <util/Platform.h>
#include <util/Network.h>
#include <array/SinglePassArray.h>
#include <array/SynchableArray.h>
#include <array/PinBuffer.h>

#include "UnparseTemplateParser.h"

#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>

#include "AioSaveSettings.h"


#define THROW_NOT_OK(s)                                            \
    {                                                              \
        arrow::Status _s = (s);                                    \
        if (!_s.ok())                                              \
        {                                                          \
            throw USER_EXCEPTION(                                  \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION) \
                    << _s.ToString().c_str();                      \
        }                                                          \
    }

#define THROW_NOT_OK_FILE(s)                                      \
    {                                                             \
        arrow::Status _s = (s);                                   \
        if (!_s.ok())                                             \
        {                                                         \
            throw USER_EXCEPTION(                                 \
                SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR) \
                    << _s.ToString().c_str() << (int)_s.code();   \
        }                                                         \
    }

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

std::shared_ptr<arrow::Schema> attributes2ArrowSchema(Attributes const& attrs)
{
    size_t noAttrs = attrs.size();

    std::vector<std::shared_ptr<arrow::Field>> arrowFields(noAttrs);
    for(size_t i = 0; i < noAttrs; ++i)
    {
        auto type = attrs[i].getType();
        auto typeEnum = typeId2TypeEnum(type, true);
        std::shared_ptr<arrow::DataType> arrowType;

        switch (typeEnum)
        {
        case TE_INT64:
            arrowType = arrow::int64();
            break;
        default:
            ostringstream error;
            error << "Type " << type << " not supported in arrow format";
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
        }

        arrowFields[i] = arrow::field(attrs[i].getName(), arrowType);
    }
    return arrow::schema(arrowFields);
}

// Workaround for https://issues.apache.org/jira/browse/ARROW-2179
// Addapted from arrow/util/io-util.h
// Output stream that just writes to stdout.
class StdoutStream : public arrow::io::OutputStream {
 public:
  StdoutStream() : pos_(0) { set_mode(arrow::io::FileMode::WRITE); }
  ~StdoutStream() override {}

  arrow::Status Close() override { return arrow::Status::OK(); }

  arrow::Status Tell(int64_t* position) const override {
    *position = pos_;
    return arrow::Status::OK();
  }

  arrow::Status Write(const void* data, int64_t nbytes) override {
    pos_ += nbytes;
    std::cout.write(reinterpret_cast<const char*>(data), nbytes);
    return arrow::Status::OK();
  }

 private:
  int64_t pos_;
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
    static const size_t s_startingSize = 8*1024*1024 + 512;

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
            memcpy(&(buf[0]), _chunk.getData(), mySize);
            _chunk.allocate(_allocSize);
            _chunkStartPointer = (char*) _chunk.getData();
            memcpy(_chunkStartPointer, &(buf[0]), mySize);
            _dataStartPointer = _chunkStartPointer + AioSaveSettings::chunkDataOffset();
            _sizePointer = (uint32_t*) (_chunkStartPointer + AioSaveSettings::chunkSizeOffset());
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

    Coordinates const& getPosition()
    {
        return _inputChunkIters[0]->getPosition();
    }
};

class BinaryChunkPopulator
{
private:
    ExchangeTemplate _templ;
    size_t const     _nAttrs;
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
        _templ(TemplateParser::parse(inputArrayDesc, settings.getBinaryFormatString(), false)),
        _nAttrs(inputArrayDesc.getAttributes(true).size()),
        _nColumns(_templ.columns.size()),
        _cnvValues(_nAttrs),
        _padBuffer(sizeof(uint64_t) + 1, '\0')
    {
        for (size_t c = 0, i = 0; c < _nColumns; ++c)
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
            for (size_t c = 0, i = 0; c < _nColumns; ++c)
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
            cursor.advance();
            ++nCells;
        }
    }
};

class ArrowChunkPopulator
{

private:
    Attributes const&                                 _inputAttrs;
    std::vector<TypeEnum>                             _inputTypes;
    std::shared_ptr<arrow::Schema>                    _arrowSchema;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> _arrowBuilders;
    std::vector<std::shared_ptr<arrow::Array>>        _arrowArrays;
    arrow::MemoryPool*                                _arrowPool = arrow::default_memory_pool();

public:
    ArrowChunkPopulator(ArrayDesc const& inputArrayDesc,
                        AioSaveSettings const& settings):
        _inputAttrs(inputArrayDesc.getAttributes(true)),
        _arrowSchema(attributes2ArrowSchema(_inputAttrs))
    {
        const size_t noAttrs = _inputAttrs.size();

        _inputTypes.resize(noAttrs);
        _arrowBuilders.resize(noAttrs);
        _arrowArrays.resize(noAttrs);

        // Create Arrow Builders
        for(size_t i = 0; i < noAttrs; ++i) {
            _inputTypes[i] = typeId2TypeEnum(_inputAttrs[i].getType(), true);

	    THROW_NOT_OK(
		arrow::MakeBuilder(
		    _arrowPool,
                    _arrowSchema->field(i)->type(),
                    &_arrowBuilders[i]));
        }
    }

    ~ArrowChunkPopulator()
    {}

    void populateChunk(MemChunkBuilder& builder,
                       ArrayCursor& cursor,
                       size_t const bytesPerChunk,
                       int16_t const cellsPerChunk)
    {
        // Basic setup
        const size_t noAttrs = _inputTypes.size();

        // Append to Arrow Builders
        int64_t nCells = 0;
        while (!cursor.end() && ((cellsPerChunk<=0 && builder.getTotalSize() < bytesPerChunk) || (cellsPerChunk > 0 && nCells < cellsPerChunk)))
        {
            vector<Value const*> const& cell = cursor.getCell();
            for (size_t i = 0; i < cursor.nAttrs(); ++i)
            {
                Value const* value = cell[i];
                switch (_inputTypes[i])
                {
                case TE_INT64:
                     if(value->isNull())
                     {
			 THROW_NOT_OK(
			     static_cast<arrow::Int64Builder*>(
				 _arrowBuilders[i].get())->AppendNull());
                     }
                     else
                     {
			 THROW_NOT_OK(
			     static_cast<arrow::Int64Builder*>(
				 _arrowBuilders[i].get())->Append(
				     value->getInt64()));
                     }
                     break;
                default:
                     ostringstream error;
                     error << "Type " << _inputAttrs[i].getType() << " not supported in arrow format";
                     throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION) << error.str();
                }
            }
            cursor.advance();
            ++nCells;
        }

        // Finalize Arrow Builders and populate Arrow Arrays
        for (size_t i = 0; i < noAttrs; ++i) {
	    THROW_NOT_OK(
		_arrowBuilders[i]->Finish(&_arrowArrays[i])); // Resets builder
        }

        // Create Arrow Record Batch
        std::shared_ptr<arrow::RecordBatch> arrowBatch;
        arrowBatch = arrow::RecordBatch::Make(_arrowSchema, nCells, _arrowArrays);

        // Stream Arrow Record Batch to Arrow Pool Buffer using Arrow Record
        // Batch Writer and Arrow Buffer Output Stream
        std::shared_ptr<arrow::PoolBuffer> arrowBuffer(
	    new arrow::PoolBuffer(_arrowPool));
        // std::shared_ptr<arrow::PoolBuffer> arrowBuffer =
        //   make_shared<arrow::PoolBuffer>(_arrowPool);
        arrow::io::BufferOutputStream arrowStream(arrowBuffer);
        std::shared_ptr<arrow::ipc::RecordBatchWriter> arrowWriter;
	THROW_NOT_OK(
	    arrow::ipc::RecordBatchStreamWriter::Open(
		&arrowStream, _arrowSchema, &arrowWriter));
	THROW_NOT_OK(
	    arrowWriter->WriteRecordBatch(*arrowBatch));
        THROW_NOT_OK(arrowWriter->Close());
        THROW_NOT_OK(arrowStream.Close());

        // Copy data to Mem Chunk Builder
        builder.addData(reinterpret_cast<const char*>(arrowBuffer->data()),
                        arrowBuffer->size());
    }
};

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
    AioSaveSettings const&  _settings;
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
        for (size_t i = 0; i < inputAttrs.size(); ++i)
        {
            if(inputAttrs[i].getType() == TID_STRING)
            {
                _attTypes[i] = STRING;
            }
            else if(inputAttrs[i].getType() == TID_BOOL)
            {
                _attTypes[i] = BOOL;
            }
            else if(inputAttrs[i].getType() == TID_DOUBLE)
            {
                _attTypes[i] = DOUBLE;
            }
            else if(inputAttrs[i].getType() == TID_FLOAT)
            {
                _attTypes[i] = FLOAT;
            }
            else if(inputAttrs[i].getType() == TID_UINT8)
            {
                _attTypes[i] = UINT8;
            }
            else if(inputAttrs[i].getType() == TID_INT8)
            {
                _attTypes[i] = INT8;
            }
            else
            {
                _converters[i] = FunctionLibrary::getInstance()->findConverter(
                    inputAttrs[i].getType(),
                    TID_STRING,
                    false);
            }
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
    map<InstanceID, string> const&           _instanceMap;
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
typedef ConversionArray <ArrowChunkPopulator>  ArrowConvertedArray;
typedef ConversionArray <TextChunkPopulator>   TextConvertedArray;

struct AwIoError
{
    AwIoError(int x) : error(x) {}
    int     error;
};

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
            for(size_t i =0; i<inputSchema.getAttributes(true).size(); ++i)
            {
                if(i || settings.printCoordinates())
                {
                    header<<settings.getAttributeDelimiter();
                }
                header<<inputSchema.getAttributes(true)[i].getName();
            }
            header<<settings.getLineDelimiter();
            ::fprintf(f, "%s", header.str().c_str());
        }
        shared_ptr<ConstArrayIterator> arrayIter = array->getConstIterator(0);
        for (size_t n = 0; !arrayIter->end(); n++)
        {
            ConstChunk const& ch = arrayIter->getChunk();
            PinBuffer scope(ch);
            uint32_t* sizePointer = (uint32_t*) (((char*)ch.getData()) + AioSaveSettings::chunkSizeOffset());
            uint32_t size = *sizePointer;
            bytesWritten += size;
            char* data = ((char*)ch.getData() + AioSaveSettings::chunkDataOffset());
            if (::fwrite(data, 1, size, f) != size)
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

uint64_t saveToDiskArrow(shared_ptr<Array> const& array,
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
        arrowStream.reset(new StdoutStream());
        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION)
             << "stdout or console not supported for arrow format";
    }
    else if (fileName == "stderr")
    {
        // TODO: StderrStream support
        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_ILLEGAL_OPERATION)
             << "stderr not supported for arrow format";
    }
    else
    {
        std::shared_ptr<arrow::io::FileOutputStream> arrowFile;
        auto arrowStatus = arrow::io::FileOutputStream::Open(
	    fileName, append, &arrowFile);
        if (!arrowStatus.ok())
        {
            auto str = arrowStatus.ToString().c_str();
            int code = (int)arrowStatus.code();
            LOG4CXX_DEBUG(logger, "Attempted to open output file '" << fileName << "' failed: " << str << " (" << code << ")");
            throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_CANT_OPEN_FILE)
                 << fileName << str << code;
        }
        arrowStream = arrowFile;
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
            inputSchema.getAttributes(true));
        THROW_NOT_OK_FILE(
            arrow::ipc::RecordBatchStreamWriter::Open(
                arrowStream.get(), arrowSchema, &arrowWriter));

        shared_ptr<ConstArrayIterator> arrayIter = array->getConstIterator(0);
        for (size_t n = 0; !arrayIter->end(); n++)
        {
            ConstChunk const& ch = arrayIter->getChunk();
            PinBuffer scope(ch);
            uint32_t* sizePointer = (uint32_t*) (((char*)ch.getData()) +
                                                 AioSaveSettings::chunkSizeOffset());
            uint32_t size = *sizePointer;
            bytesWritten += size;
            char* data = ((char*)ch.getData() + AioSaveSettings::chunkDataOffset());

            arrow::io::BufferReader arrowBufferReader(
                reinterpret_cast<const uint8_t*>(data), size); // zero copy

	    // Read Record Batch
            // THROW_NOT_OK(
            //     arrow::ipc::ReadRecordBatch(
            //         arrowSchema, &arrowBufferReader, &arrowBatch));

            // Read Record Batch using Stream Reader
	    THROW_NOT_OK(
	        arrow::ipc::RecordBatchStreamReader::Open(
	            &arrowBufferReader, &arrowReader));
            THROW_NOT_OK(arrowReader->ReadNext(&arrowBatch));

	    // Write Record Batch to stream
	    THROW_NOT_OK_FILE(
		arrowWriter->WriteRecordBatch(*arrowBatch));

	    ++(*arrayIter);
        }
    }
    catch (AwIoError& e)
    {
        if (arrowWriter != nullptr)
        {
            arrowWriter->Close();
        }
        if (fileName == "console" || fileName == "stdout")
        {
            arrowStream->Flush();
        }
        else
        {
            arrowStream->Close();
        }
        throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR)
	    << ::strerror(e.error) << e.error;
    }

    LOG4CXX_DEBUG(logger, "ALT_SAVE>> wrote "<< bytesWritten<< " bytes, closing");
    THROW_NOT_OK_FILE(arrowWriter->Close());
    if (fileName == "console" || fileName == "stdout")
    {
        THROW_NOT_OK_FILE(arrowStream->Flush());
    }
    else
    {
        THROW_NOT_OK_FILE(arrowStream->Close());
    }
    LOG4CXX_DEBUG(logger, "ALT_SAVE>> closed")
    return 0;
}


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

    bool haveChunk(shared_ptr<Array>& input)
    {
        shared_ptr<ConstArrayIterator> iter = input->getConstIterator(0);
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
        *((bool*) buf->getData()) = value;
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
                bool otherInstanceVal = *((bool*) buf->getData());
                value = value && otherInstanceVal;
            }
        }
        return value;
    }

    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        AioSaveSettings settings (_parameters, false, query);
        shared_ptr<Array>& input = inputArrays[0];
        ArrayDesc const& inputSchema = input->getArrayDesc();
        bool singleChunk = isSingleChunk(inputSchema);
        shared_ptr< Array> outArray;
        if(settings.isBinaryFormat())
        {
            outArray.reset(new BinaryConvertedArray(_schema, input, query, settings));
        }
        else if(settings.isArrowFormat())
        {
            outArray.reset(new ArrowConvertedArray(_schema, input, query, settings));
        }
        else
        {
            outArray.reset(new TextConvertedArray(_schema, input, query, settings));
        }
        InstanceID const myInstanceID = query->getInstanceID();
        map<InstanceID, string>::const_iterator iter = settings.getInstanceMap().find(myInstanceID);
        bool thisInstanceSavesData = (iter != settings.getInstanceMap().end());
        if(singleChunk && agreeOnBoolean((thisInstanceSavesData == haveChunk(input)), query))
        {
            LOG4CXX_DEBUG(logger, "ALT_SAVE>> single-chunk path")
            if(thisInstanceSavesData)
            {
                string const& path = iter->second;
		if (settings.isArrowFormat())
		{
		    saveToDiskArrow(
			outArray, path, query, false, settings, inputSchema);
		}
		else
		{
		    saveToDisk(
			outArray, path, query, false, settings, inputSchema);
		}
            }
            return shared_ptr<Array>(new MemArray(_schema, query));
        }
        shared_ptr<Array> outArrayRedist;
        LOG4CXX_DEBUG(logger, "ALT_SAVE>> Starting SG")
        outArrayRedist = pullRedistribute(outArray,
                                          createDistribution(psByCol),
                                          ArrayResPtr(),
                                          query,
                                          getShared());
        bool const wasConverted = (outArrayRedist != outArray) ;
        if (thisInstanceSavesData)
        {
            string const& path = iter->second;
	    if (settings.isArrowFormat())
	    {
		saveToDiskArrow(
		    outArrayRedist, path, query, false, settings, inputSchema);
	    }
	    else
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
        return shared_ptr<Array>(new MemArray(_schema, query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalAioSave, "aio_save", "PhysicalAioSave");

} // end namespace scidb
