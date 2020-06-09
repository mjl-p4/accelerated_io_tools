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

#define LEGACY_API
#include <limits>
#include <sstream>

#include <boost/unordered_map.hpp>
#include <query/PhysicalOperator.h>
#include <util/Platform.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <array/SinglePassArray.h>
#include <array/PinBuffer.h>
#include <system/Sysinfo.h>
#include <network/Network.h>

#include <boost/algorithm/string.hpp>

#include <fcntl.h>
#include <string.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "AioInputSettings.h"

using std::make_shared;

using boost::algorithm::is_from_range;

namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.alt_load"));

using namespace scidb;

static size_t getChunkOverheadSize()
{
    return             (  sizeof(ConstRLEPayload::Header) +
                                 2 * sizeof(ConstRLEPayload::Segment) +
                                 sizeof(varpart_offset_t) + 5);
}

static size_t getSizeOffset()
{
    return getChunkOverheadSize()-4;
}

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
    size_t const _fileBlockSize;
    size_t const _chunkOverheadSize;
    bool         _endOfFile;
    char*        _bufPointer;
    uint32_t*    _sizePointer;
    int          _inputFile;
    size_t const _nInstances;
    ssize_t       _chunkNo;

public:
    BinFileSplitArray(ArrayDesc const& schema,
                      shared_ptr<Query>& query,
                      shared_ptr<AioInputSettings> settings):
        super(schema),
        _rowIndex(0),
        _chunkAddress(0, Coordinates(3,0)),
        _query(query),
        _fileBlockSize(settings->getBlockSize()),
        _chunkOverheadSize( getChunkOverheadSize() ),
        _endOfFile(false),
        _inputFile(-1),  // invalid FD
        _nInstances(query->getInstancesCount()),
        _chunkNo(0)
    {
        super::setEnforceHorizontalIteration(true);
        _chunkAddress.coords[2] = query->getInstanceID();
        try
        {
            _chunk.allocate(_chunkOverheadSize + _fileBlockSize);
        }
        catch(...)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "File splitter cannot allocate memory";
        }
        _bufPointer = (char*) _chunk.getWriteData();
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
        string const& filePath = settings->getInputFilePath();
        _inputFile = openFile(filePath, query->getInstanceID());
        int const header = settings->getHeader();
        if (header > 0) {
            skipHeader(_inputFile, header, settings->getLineDelimiter(), query);
        }
    }

    /**
     * Open the input file for reading.
     *
     * @param filePath The path to the FIFO, pipe, or file to read input from.
     * @param instanceId The ID of this instance, used only for the exception message
     *    should this openFile fail.
     */
    static int openFile(std::string const& filePath,
                        InstanceID instanceId)
    {
        // Mark this FD as non-blocking because filePath could refer to any
        // file-like device and we must be able to periodically check for
        // a valid query.  If we marked this as blocking, then we will end
        // up stuck forever in a read() in cases where pipes that don't
        // have data yet in them but the query has been cancelled.
        auto fd = open(filePath.c_str(), O_RDONLY | O_NONBLOCK);

        if (fd == -1) {
	    int err = errno;
            ostringstream errorMsg;
            errorMsg << "cannot open file '" << filePath << "' on instance " << instanceId
		     << " (errno=" << err << ", '" << strerror(err) << "')";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << errorMsg.str().c_str();
        }

        return fd;
    }

    virtual ~BinFileSplitArray()
    {
        if (_inputFile != -1)
        {
            ::close(_inputFile);
        }
    }

    size_t getCurrentRowIndex() const
    {
        return _rowIndex;
    }

    /**
     * Read past some number of lines from the input file, regarding
     * them as a header containing no data to load.
     *
     * @param fd The input file's descriptor.
     * @param linesToSkip The number of lines in the header to disregard.
     * @param lineDelim The character delimiting lines in the input file.
     * @param query A weak pointer to the query.
     */
    static void skipHeader(int fd,
                           int linesToSkip,
                           char lineDelim,
                           std::weak_ptr<Query> query)
    {
        // Linux implementation of getdelim() reads a single byte-at-a-time
        // from the input.  Do that for now, optimizing later if we need to.
        char buf;
        int linesSkipped = 0;
        do {
            auto result = scidb_read(fd, &buf, 1, query);
            if (result == 1 && buf == lineDelim) {
                ++linesSkipped;
            }
        } while (linesSkipped < linesToSkip);
    }

    /**
     * Read from the input, allowing query cancellation to interrupt
     * the read, ensuring that array locks are cleaned-up and the query
     * aborted appropriately.
     *
     * @param fd The input file's descriptor.
     * @param buf The buffer into which to read data.
     * @param count The size of the buffer in bytes.
     * @param query A weak pointer to the query.
     */
    static ssize_t scidb_read(int fd,
                              void* buffer,
                              size_t const count,
                              std::weak_ptr<Query> query)
    {
        ssize_t total = 0;
        size_t rdCnt = count;
        struct stat fdStat;
        fstat(fd, &fdStat);
        const bool isFifo = S_ISFIFO(fdStat.st_mode);
        fd_set readFdSet;
        auto buf = static_cast<char*>(buffer);
        do {
            FD_ZERO(&readFdSet);
            FD_SET(fd, &readFdSet);
            // select can change the timeval parameter, so make a new
            // copy of it each time around the loop.
            timeval rdTimeout{/*tv_sec:*/1, /*tv_usec:*/0};
            int fds_ready = select(fd+1, &readFdSet, nullptr, nullptr, &rdTimeout);
            if (fds_ready > 0) {
                if (FD_ISSET(fd, &readFdSet)) {
                    ssize_t nb = ::read(fd, buf, rdCnt);
                    if (nb > 0) {
                        // Making progress on the read.
                        total += nb;
                        rdCnt -= nb;
                        buf += nb;
                    }
                    else if (nb == 0) {
                        if (isFifo && total == 0) {
                            // We haven't read any data from the FIFO yet.  To preserve
                            // the previous aio_input behavior, don't return, and try
                            // again.  But first, check the query to be sure we haven't
                            // aborted.  If we have aborted, then this call will throw.
                            Query::getValidQueryPtr(query);
                            continue;
                        }
                        // else, we've read data to the end of the FIFO or file.
                        return total;
                    }
                    else {  // nb < 0
                        if (errno != EAGAIN) {
                            // Some other signal interrupted, so return what we have.
                            return total;
                        }
                        // else try again on EAGAIN
                    }
                }
                else {
                    // else some other FD is set.  This shouldn't be possible
                    // at the commit where this scidb_read was introduced
                    // because we passed on only one FD to select().
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                        << "Unexpected file descriptor in aio_input() has data available";
                }
            }
            else if (fds_ready == 0) {
                // No fds ready, but maybe the query died, so check
                // that.  If the query is dead, then this will throw,
                // prompting the input operation to terminate.
                Query::getValidQueryPtr(query);
            }
            else {  // else fds_ready < 0
                // An error occurred during select().
                return total;
            }
        } while (rdCnt > 0);

        return total;
    }

    bool moveNext(size_t rowIndex)
    {
        if (_endOfFile) {
            return false;
        }
        ssize_t numBytes = scidb_read(_inputFile, _bufPointer, _fileBlockSize, _query);

        if (numBytes == -1) {
            // Error, inspect errno and abort the query with an exception.
            ostringstream oss;
            oss << "aio_input() error reading from fd, errno="
                << errno << " (" << strerror(errno) << ")";
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                << oss.str().c_str();
        }

        if (numBytes != _fileBlockSize) {
            _endOfFile = true;
            ::close(_inputFile);
            _inputFile = -1;
            if (numBytes == 0) {
                return false;
            }
            *_sizePointer = (uint32_t) numBytes;
        }
        ++_rowIndex;
        return true;
    }

    ConstChunk const& getChunk(AttributeID attr, size_t rowIndex)
    {
        _chunkAddress.coords[1] = (_rowIndex  - 1 + _chunkAddress.coords[2]) % _nInstances;
        if(_chunkAddress.coords[1] == 0 && _rowIndex > 1)
        {
            _chunkNo++;
        }
        _chunkAddress.coords[0] = _chunkNo;
        shared_ptr<Query> query = Query::getValidQueryPtr(_query);
        _chunk.initialize(this, &super::getArrayDesc(), _chunkAddress, CompressorType::NONE);
        return _chunk;
    }
};

class AIOOutputWriter : public boost::noncopyable
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
    vector<Value> _buf;
    ostringstream _errorBuf;
    Value _errorBufVal;
    Coordinate _outputPositionLimit;

public:
    AIOOutputWriter(ArrayDesc const& schema, shared_ptr<Query>& query, bool splitOnDimension, char const attDelimiter):
        _output(std::make_shared<MemArray>(schema,query)),
        _outputPosition( splitOnDimension ? 4 : 3, 0),
        _numLiveAttributes(schema.getAttributes(true).size()),
        _outputLineSize(splitOnDimension ? schema.getDimensions()[3].getChunkInterval() : _numLiveAttributes),
        _outputChunkSize(schema.getDimensions()[0].getChunkInterval()),
        _outputArrayIterators(_numLiveAttributes),
        _outputChunkIterators(_numLiveAttributes),
        _splitOnDimension(splitOnDimension),
        _outputColumn(0),
        _attributeDelimiter(attDelimiter),
        _buf(_outputLineSize-1)
    {
/*        for(AttributeID i =0; i<_numLiveAttributes; ++i)
        {
            _outputArrayIterators[i] = _output->getIterator(i);
            }*/
        for (const auto& attr : schema.getAttributes(/*excludeEbm:*/true))
        {
            _outputArrayIterators[attr.getId()] = _output->getIterator(attr);
        }
    }

    void newChunk (Coordinates const& inputChunkPosition, shared_ptr<Query>& query)
    {
        _outputPosition[0] = inputChunkPosition[0] * _outputChunkSize;
        _outputPositionLimit = _outputPosition[0] + _outputChunkSize;
        _outputPosition[1] = inputChunkPosition[1];
        _outputPosition[2] = inputChunkPosition[2];
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
        if(_outputPosition[0] >= _outputPositionLimit)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "The chunk size is too small for the current block size. Lower the block size or increase chunk size";
        }
        if(_outputColumn < _outputLineSize - 1)
        {
            Value& buf = _buf[_outputColumn];
            buf.setSize<Value::IGNORE_DATA>(end - start + 1);
            char* d = buf.getData<char>();
            memcpy(d, start, end-start);
            d[(end-start)]=0;
            if(_splitOnDimension)
            {
                _outputChunkIterators[0] -> setPosition(_outputPosition);
                _outputChunkIterators[0] -> writeItem(buf);
                ++(_outputPosition[3]);
            }
            else
            {
                _outputChunkIterators[_outputColumn] -> setPosition(_outputPosition);
                _outputChunkIterators[_outputColumn] -> writeItem(buf);
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
            _errorBufVal.setNull();
            if(_splitOnDimension)
            {
                while (_outputColumn < _outputLineSize - 1)
                {
                    _outputChunkIterators[0] -> setPosition(_outputPosition);
                    _outputChunkIterators[0] -> writeItem(_errorBufVal);
                    ++(_outputPosition[3]);
                    ++_outputColumn;
                }
            }
            else
            {
                while (_outputColumn < _outputLineSize - 1)
                {
                    _outputChunkIterators[_outputColumn] -> setPosition(_outputPosition);
                    _outputChunkIterators[_outputColumn] -> writeItem(_errorBufVal);
                    ++_outputColumn;
                }
            }
            _errorBuf << "short";
        }
        if(_errorBuf.str().size())
        {
            _errorBufVal.setString(_errorBuf.str());
            _errorBuf.str("");
        }
        else
        {
            _errorBufVal.setNull();
        }
        if(_splitOnDimension)
        {
            _outputChunkIterators[0] -> setPosition(_outputPosition);
            _outputChunkIterators[0] -> writeItem(_errorBufVal);
            _outputPosition[3] = 0;
        }
        else
        {
            _outputChunkIterators[_outputLineSize - 1] -> setPosition(_outputPosition);
            _outputChunkIterators[_outputLineSize - 1] -> writeItem(_errorBufVal);
        }
        ++(_outputPosition[0]);
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

/**
 * Class AIOOutputCache
 *
 * Tracks all of the pieces of the input buffer that make up the chunks
 * written for any given line in the input file.  Allows us to know
 * before we write any chunks for a line of the input if that line has
 * an error that would cause the 'error' attribute not to be null.
 *
 * Assumes that the lifetime of the buffer read from the file is longer
 * than the lifetime of this object.
 */
class AIOOutputCache
{
private:
    size_t const _outputLineSize;
    size_t _outputColumn;
    AioInputSettings::Skip _skip;
    bool _hasError{false};

    // An operation on the output array based-on the input data.
    struct Operation
    {
        enum class Type
        {
            NONE,
            NEW_CHUNK,   // corresponds to AIOOutputWriter::newChunk
            WRITE_VALUE, // corresponds to AIOOutputWriter::writeValue
            END_LINE     // corresponds to AIOOutputWriter::endLine
        };

        Operation(Type type,
                  Coordinates const* inputChunkPos,
                  char const* start,
                  char const* end)
                : _type(type)
                , _inputChunkPosition(nullptr)
                , _start(start)
                , _end(end)
        {
            if (inputChunkPos) {
                _inputChunkPosition = std::make_shared<Coordinates>(*inputChunkPos);
            }
        }

        Type _type{Type::NONE};
        std::shared_ptr<Coordinates> _inputChunkPosition;
        const char* _start{nullptr};
        const char* _end{nullptr};
    };

    // All of the AIOOutputWriter operations that would've been done
    // inline while reading the input file are instead remembered here,
    // then inspected before any writing to the output array chunks takes
    // place, allowing the 'skip' parameter to have its effect if configured.
    using Operations = std::vector<Operation>;
    Operations _operations;
    Operations _currLine;

public:
    AIOOutputCache(ArrayDesc const& schema,
                   shared_ptr<Query>& query,
                   bool splitOnDimension,
                   AioInputSettings::Skip skip)
        : _outputLineSize(splitOnDimension ?
                          schema.getDimensions()[3].getChunkInterval() :
                          schema.getAttributes(true).size())
        , _outputColumn(0)
        , _skip(skip)
        , _operations()
        , _currLine()
    { }

    /**
     * This duplicates the initialization and setup of the AIOOutputWriter::newChunk
     * method but without committing anything to the output chunks.
     *
     * @param inputChunkPosition The upper left corner of the input chunk.
     * @param query The query.
     */
    void newChunk(Coordinates const& inputChunkPosition, shared_ptr<Query>& query)
    {
        _operations.emplace_back(Operation::Type::NEW_CHUNK, &inputChunkPosition, nullptr, nullptr);
    }

    /**
     * This duplicates the column and offset math of the AIOOutputWriter::writeValue
     * method but without committing anything to the output chunks, allowing us to
     * know if there would be an error at this line before writing to the output array.
     *
     * @param start A pointer to the start of a memory region containing the value
     *    to write to the next output chunk.
     * @param end  A pointer to the end of the memory region containing the value
     *    to write to the next output chunk.
     */
    void writeValue(char const* start, char const* end)
    {
        if (_outputColumn >= _outputLineSize - 1) {
            _hasError = true;
        }

        ++_outputColumn;

        _currLine.emplace_back(Operation::Type::WRITE_VALUE, nullptr, start, end);
    }

    /**
     * This duplicates the column and offset math of the AIOOutputWriter::endLine
     * method but without committing anything to the output chunks, allowing us to
     * know if there would be an error at this line before writing to the output array.
     */
    void endLine()
    {
        if (_outputColumn < _outputLineSize - 1) {
            _hasError = true;
        }

        _outputColumn = 0;

        _currLine.emplace_back(Operation::Type::END_LINE, nullptr, nullptr, nullptr);

        // endLine() is called only once per line of input.  If there's an
        // error at this point, then depending on the 'skip' parameter,
        // delete this line from the cache.  When the cache is replayed later
        // to write the chunks to the output array, the deleted line won't appear
        // in the output.
        if ((_skip == AioInputSettings::Skip::ERRORS && _hasError)
            || (_skip == AioInputSettings::Skip::NON_ERRORS && !_hasError)) {
            _currLine.clear();
        }
        else {
            _operations.insert(_operations.end(),
                               _currLine.begin(),
                               _currLine.end());
            _currLine.clear();
        }
        _hasError = false;
    }

    /**
     * Playback all of the operations recorded while processing the lines
     * from the input chunk of the file, modulo any dropped lines due to
     * an error, depending on the 'skip' parameter.
     *
     * @param query The query.
     * @param writer The instance of the AIOOutputWriter that owns the
     *     output array and its chunks.
     */
    void playback(std::shared_ptr<Query> query,
                  AIOOutputWriter& writer)
    {
        if (_operations.size() == 1) {
            // Every line on this chunk had an error, and
            // only the NEW_CHUNK operation remains
            // for playback.  Don't execute it, because
            // executing it would create a chunk that has
            // nothing on it.
            return;
        }

        for (auto& op : _operations) {
            switch (op._type) {
            case Operation::Type::NEW_CHUNK:
                writer.newChunk(*op._inputChunkPosition, query);
                break;
            case Operation::Type::WRITE_VALUE:
                writer.writeValue(op._start, op._end);
                break;
            case Operation::Type::END_LINE:
                writer.endLine();
                break;
            case Operation::Type::NONE:
                // fall-through to default
            default:
                throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION)
                    << "Invalid operation type 'NONE' during aio_input()";
                break;  // unreachable
            }
        }

        _currLine.clear();
        _operations.clear();
    }
};

class PhysicalAioInput : public PhysicalOperator
{
public:
    static ArrayDesc getSplitSchema(shared_ptr<Query> & query)
    {
        size_t const nInstances = query->getInstancesCount();
        vector<DimensionDesc> dimensions(3);
        dimensions[0] = DimensionDesc("chunk_no",           0, 0, CoordinateBounds::getMax(), CoordinateBounds::getMax(), 1, 0);
        dimensions[1] = DimensionDesc("dst_instance_id",    0, 0, nInstances-1, nInstances-1, 1, 0);
        dimensions[2] = DimensionDesc("src_instance_id",    0, 0, nInstances-1, nInstances-1, 1, 0);
        Attributes attributes;
        attributes.push_back(AttributeDesc("value",  TID_BINARY, 0, CompressorType::NONE));
        return ArrayDesc("aio_input", attributes, dimensions, createDistribution(defaultDistType()), query->getDefaultArrayResidency());
    }

    PhysicalAioInput(std::string const& logicalName,
                     std::string const& physicalName,
                     Parameters const& parameters,
                     ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    virtual bool changesDistribution(std::vector<ArrayDesc> const&) const
    {
        return true;
    }

    virtual RedistributeContext getOutputDistribution(
            std::vector<RedistributeContext> const& inputDistributions,
            std::vector< ArrayDesc> const& inputSchemas) const
    {
        RedistributeContext distro(_schema.getDistribution(), _schema.getResidency());
        return distro;
    }

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
        std::vector<RedistributeContext> emptyRC;
        std::vector<ArrayDesc> emptyAD;
        auto context = getOutputDistribution(emptyRC, emptyAD); // avoiding duplication of logic
        return context.getArrayDistribution()->getDistType();
    }

    shared_ptr<Array> makeSupplement(shared_ptr<Array>& afterSplit, shared_ptr<Query>& query, shared_ptr<AioInputSettings>& settings, vector<Coordinate>& lastBlocks)
    {
        char const lineDelim = settings->getLineDelimiter();
        shared_ptr<Array> supplement(new MemArray(getSplitSchema(query), query));
        shared_ptr<ConstArrayIterator> srcArrayIter = afterSplit->getConstIterator(getSplitSchema(query).getAttributes(true).firstDataAttribute());
        shared_ptr<ArrayIterator> dstArrayIter = supplement->getIterator(getSplitSchema(query).getAttributes(true).firstDataAttribute());
        shared_ptr<ChunkIterator> dstChunkIter;
        size_t const nInstances = query->getInstancesCount();
        while(!srcArrayIter->end())
        {
           Coordinates supplementCoords = srcArrayIter->getPosition();
           Coordinate block = supplementCoords[0] * nInstances + supplementCoords[1];
           Coordinate dst   = supplementCoords[1];
           Coordinate src   = supplementCoords[2];
           if(lastBlocks[src] < block)
           {
               lastBlocks[src] = block;
           }
           if(supplementCoords[0] != 0 || supplementCoords[1] != supplementCoords[2])
           {
               ConstChunk const& ch = srcArrayIter->getChunk();
               PinBuffer pinScope(ch);
               char* start = ((char*) ch.getConstData()) + getChunkOverheadSize();
               uint32_t const sourceSize = *((uint32_t*)(((char*) ch.getConstData()) + getSizeOffset()));
               if(dst == 0)
               {
                   supplementCoords[1] = nInstances-1;
                   supplementCoords[0]--;
               }
               else
               {
                   supplementCoords[1] = dst - 1;
               }
               void * tmp = memchr(start, lineDelim, sourceSize);
               if(!tmp)
               {
                   throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered a whole block without line delimiter characters; Sorry! You need to increase the block size.";
               }
               char const* cur = static_cast<char*>(tmp);
               Value firstLine;
               firstLine.setSize<Value::IGNORE_DATA>(cur-start);
               memcpy(firstLine.data(), start, cur-start);
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
            memcpy(&otherLastBlocks[0], buf->getConstData(), vectorSize);
            for(size_t j =0; j<numInstances; ++j)
            {
                if(otherLastBlocks[j] > myLastBlocks[j])
                {
                    myLastBlocks[j] = otherLastBlocks[j];
                }
            }
        }
    }

    shared_ptr< Array> execute(std::vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        shared_ptr<AioInputSettings> settings (new AioInputSettings(_parameters, _kwParameters, false, query));
        shared_ptr<Array> splitData;
        if(settings->thisInstanceReadsData())
        {
            splitData = shared_ptr<BinFileSplitArray>(new BinFileSplitArray(getSplitSchema(query), query, settings));
        }
        else
        {
            splitData = shared_ptr<BinEmptySinglePass>(new BinEmptySinglePass(getSplitSchema(query)));
        }

        splitData = redistributeToRandomAccess(splitData,
                                               createDistribution(dtHashPartitioned),
                                               ArrayResPtr(),
                                               query,
                                               shared_from_this());
        size_t const nInstances = query->getInstancesCount();
        vector<Coordinate> lastBlocks(nInstances, -1);
        shared_ptr<Array> supplement = makeSupplement(splitData, query, settings, lastBlocks);
        exchangeLastBlocks(lastBlocks, query);
        supplement = redistributeToRandomAccess(supplement,
                                                createDistribution(dtHashPartitioned),
                                                ArrayResPtr(),
                                                query,
                                                shared_from_this());
        shared_ptr<ConstArrayIterator> inputIterator = splitData->getConstIterator(getSplitSchema(query).getAttributes(true).firstDataAttribute());
        shared_ptr<ConstArrayIterator> supplementIter = supplement->getConstIterator(getSplitSchema(query).getAttributes(true).firstDataAttribute());
        size_t const outputChunkSize = _schema.getDimensions()[0].getChunkInterval();
        char const attDelim = settings->getAttributeDelimiter();
        char const lineDelim = settings->getLineDelimiter();
        AIOOutputWriter writer(_schema, query, settings->getSplitOnDimension(), settings->getAttributeDelimiter());
        AIOOutputCache cache(_schema,
                             query,
                             settings->getSplitOnDimension(),
                             settings->getSkip());
        size_t const overheadSize = getChunkOverheadSize();
        size_t const sizeOffset = getSizeOffset();
        while(!inputIterator-> end())
        {
            Coordinates const& pos = inputIterator->getPosition();
            Coordinate const block = pos[0] * nInstances + pos[1];
            bool const lastBlock = (lastBlocks[ pos[2] ] == block);
            ConstChunk const& chunk =  inputIterator->getChunk();
            {
                PinBuffer pinScope(chunk);
                char* chunkData = ((char*) chunk.getConstData());
                char* sourceStart = chunkData + overheadSize;
                char* chunkBodyStart  = sourceStart;
                uint32_t sourceSize = *((uint32_t*)(chunkData + sizeOffset));
                if(sourceSize == 0)
                {
                    throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "[defensive] encountered a chunk with no data.";
                }
                size_t nLines =0;
                if(pos[0] != 0 || pos[1] != pos[2])
                {
                    while((*sourceStart)!=lineDelim)
                    {
                        sourceStart ++;
                    }
                    sourceStart ++;
                    sourceSize = sourceSize - (sourceStart - chunkBodyStart);
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
                cache.newChunk(pos, query);
                while (!finished)
                {
                    while( end != terminus && (*end)!=attDelim && (*end)!=lineDelim )
                    {
                        ++end;
                    }
                    cache.writeValue(start, end);
                    if(end == terminus || (*end) == lineDelim )
                    {
                        cache.endLine();
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

                // Playback the cache calls made during the
                // loop above, into the output writer, in
                // the same order and with the same values as
                // they would've been executed had the cache
                // layer not been present.  This must be done
                // here and before the buf buffer goes out-of-scope,
                // otherwise the recorded addresses will point to
                // bogus data.
                cache.playback(query, writer);
            }
            ++(*inputIterator);
        }
        return writer.finalize();
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalAioInput, "aio_input", "PhysicalAioInput");

} // end namespace scidb
