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

class MemArrayAppender
{
private:
    std::shared_ptr<Query> const&      _query;
    size_t const                       _nAttrs;
    size_t const                       _chunkSize;
    Coordinates                        _cellPos;
    Coordinates                        _chunkPos;
    std::vector<std::shared_ptr<ArrayIterator> > _aiters;
    std::vector<std::shared_ptr<ChunkIterator> > _citers;
    Value _buf;

    void advance()
    {
        ++_cellPos[1];
        if (_cellPos[1] % _chunkSize == 0)
        {
            if (_citers[0])
            {
                for(size_t i=0; i<_nAttrs; ++i)
                {
                    _citers[i]->flush();
                }
            }
             _chunkPos[1] += 1;
            _citers[0]=_aiters[0] ->newChunk(_chunkPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE);
            for(size_t i=1; i<_nAttrs; ++i)
            {
                _citers[i] =_aiters[i] ->newChunk(_chunkPos).getIterator(_query, ChunkIterator::SEQUENTIAL_WRITE |
                                                                                 ChunkIterator::NO_EMPTY_CHECK);
            }
        }
        for(size_t i=0; i<_nAttrs; ++i)
        {
            _citers[i] ->setPosition(_cellPos);
        }
    }

public:
     MemArrayAppender(std::shared_ptr<MemArray> inArray,
                      std::shared_ptr<Query> const& query ):
       _query(query),
       _nAttrs(inArray->getArrayDesc().getAttributes(true).size()),
       _chunkSize(inArray->getArrayDesc().getDimensions()[0].getChunkInterval()),
       _cellPos(2,0),
       _chunkPos(2,0),
       _aiters(_nAttrs),
       _citers(_nAttrs)
    {

    	 LOG4CXX_DEBUG(logger, "FOOBAR: MemArray Appender constructor");
    	_cellPos[0]  = query->getInstanceID();
    	_chunkPos[0] = _cellPos[0];
    	_cellPos[1]  = -1;
    	_chunkPos[1] = _cellPos[1];
    	std::string cell1 = std::to_string(_cellPos[0]);
    	std::string cell2 = std::to_string(_cellPos[1]);
    	LOG4CXX_DEBUG(logger, "FOOBAR: MemArray Appender constructor- set Positions:" << cell1 << ":" << cell2);
    	for (AttributeID i = 0; i<_nAttrs; ++i)
        {
            _aiters[i] = inArray->getIterator(i);

        }
    	LOG4CXX_DEBUG(logger, "FOOBAR: MemArray Appender constructor- geIterator");
    }


    void addValues(Value const& v0)
    {
        EXCEPTION_ASSERT(_nAttrs==1);
        advance();
        _citers[0]->writeItem(v0);
    }

    void release()
    {
        if (_citers[0])
        {
            for(size_t i=0; i<_nAttrs; ++i)
            {
                _citers[i]->flush();
                _citers[i].reset();
            }
        }
        for(size_t i=0; i<_nAttrs; ++i)
        {
            _aiters[i].reset();
        }
    }
};

class MemArrayBuilder
{
private:
    std::shared_ptr<MemArray>          _out;
    MemArrayAppender                   _appender;

public:
    MemArrayBuilder (ArrayDesc const& schema,
                     std::shared_ptr<Query> const& query):
        _out(new MemArray(schema, query)),
        _appender(_out, query)
    {}


    void addValues(Value const& v0)
    {
        _appender.addValues(v0);
    }

    std::shared_ptr<Array> finalize()
    {
    	_appender.release();
        return _out;
    }
};

std::shared_ptr< Array> convertToTDV(shared_ptr<Array> const& inputArray,
                                     AltSaveSettings const& settings,
                                     MemArrayBuilder& outputwriter)
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
                outputwriter.addValues(insertBuf);
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
        outputwriter.addValues(insertBuf);
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
        MemArrayBuilder& outputWriter)
{
    ArrayDesc const& desc = inputArray->getArrayDesc();
    ExchangeTemplate templ = TemplateParser::parse(desc, settings.getBinaryFormatString(), false);
    const size_t N_ATTRS = desc.getAttributes(true /*exclude empty bitmap*/).size();
    const size_t N_COLUMNS = templ.columns.size();
    SCIDB_ASSERT(N_COLUMNS >= N_ATTRS); // One col per attr, plus "skip" columns
    vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(N_ATTRS);
    vector< std::shared_ptr<ConstChunkIterator> > chunkIterators(N_ATTRS);
    vector< Value > cnvValues(N_ATTRS);
    vector< char > padBuffer(sizeof(uint64_t) + 1, '\0'); // Big enuf for all nullable built-ins
    vector< char > chunkBuffer;
    chunkBuffer.reserve(10 * 1024 * 1024);
    size_t nMissingReasonOverflows = 0;
    Value insertBuf;
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
            arrayIterators[i] = inputArray->getConstIterator(i);
            if (column.converter)
            {
                cnvValues[i] = Value(column.externalType);
            }
            ++i;            // next attribute
        }
    }
    uint64_t nCells = 0;    // aka number of tuples written
    for (size_t n = 0; !arrayIterators[0]->end(); n++)
    {
        for (size_t i = 0; i < N_ATTRS; i++)
        {
            chunkIterators[i] = arrayIterators[i]->getChunk().getConstIterator(
                    ConstChunkIterator::IGNORE_OVERLAPS |
                    ConstChunkIterator::IGNORE_EMPTY_CELLS);
        }
        while (!chunkIterators[0]->end())
        {
            for (size_t c = 0, i = 0; c < N_COLUMNS; ++c)
            {
                ExchangeTemplate::Column const& column = templ.columns[c];
                if (column.skip)
                {
                    size_t pad = skip_bytes(column);
                    SCIDB_ASSERT(padBuffer.size() >= pad);
                    chunkBuffer.insert (chunkBuffer.end(), padBuffer.begin(), padBuffer.end());
                }
                else
                {
                    Value const* v = &chunkIterators[i]->getItem();
                    if (column.nullable)
                    {
                        if (v->getMissingReason() > 127)
                        {
                            LOG4CXX_WARN(logger, "Missing reason " << v->getMissingReason() << " cannot be stored in binary file");
                            nMissingReasonOverflows += 1;
                        }
                        int8_t missingReason = (int8_t)v->getMissingReason();
                        chunkBuffer.insert (chunkBuffer.end(), &missingReason, &missingReason + sizeof(missingReason));
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
                        chunkBuffer.insert (chunkBuffer.end(), padBuffer.begin(), padBuffer.begin() + size);
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
                            chunkBuffer.insert (chunkBuffer.end(), (char *) &size,    (char *) &size + sizeof(size));
                            chunkBuffer.insert (chunkBuffer.end(), (char *)v->data(), (char *)v->data() + size);
                        }
                        else
                        {
                            if (size > column.fixedSize)
                            {
                                throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION) << size << column.fixedSize;
                            }
                            chunkBuffer.insert (chunkBuffer.end(), (char *)v->data(), (char *)v->data() + size);
                            if (size < column.fixedSize)
                            {
                                size_t padSize = column.fixedSize - size;
                                assert(padSize <= padBuffer.size());
                                chunkBuffer.insert (chunkBuffer.end(), padBuffer.begin(), padBuffer.begin() + padSize);
                            }
                        }
                    }
                    ++(*chunkIterators[i]);
                    ++i;
                }
                ++nCells;
                if(nCells>settings.getLinesPerChunk())
                {
                    insertBuf.setData((char *)&(chunkBuffer[0]), chunkBuffer.size());
                    outputWriter.addValues(insertBuf);
                    chunkBuffer.clear();
                    nCells = 0;
                }
            }
        }
        for (size_t i = 0; i < N_ATTRS; i++)
        {
            ++(*arrayIterators[i]);
        }
    }
    if(nCells)
    {
        insertBuf.setData((char *)&(chunkBuffer[0]), chunkBuffer.size());
        outputWriter.addValues(insertBuf);
    }
    return outputWriter.finalize();
}

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
        vector< std::shared_ptr<ConstArrayIterator> > arrayIterators(N_ATTRS);
        for (size_t i = 0; i < N_ATTRS; i++)
        {
            arrayIterators[i] = array->getConstIterator(i);
        }
        vector< std::shared_ptr<ConstChunkIterator> > chunkIterators(N_ATTRS);
        vector< Value > cnvValues(N_ATTRS);
        uint64_t nCells = 0;    // aka number of tuples written
        for (size_t n = 0; !arrayIterators[0]->end(); n++)
        {
            for (size_t i = 0; i < N_ATTRS; i++)
            {
                chunkIterators[i] = arrayIterators[i]->getChunk().getConstIterator( ConstChunkIterator::IGNORE_OVERLAPS | ConstChunkIterator::IGNORE_EMPTY_CELLS);
            }
            while (!chunkIterators[0]->end())
            {
                ++nCells;
                int i = 0;
                Value const* v = &chunkIterators[i]->getItem();
                if (v->size() > numeric_limits<uint32_t>::max())
                {
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_TRUNCATION) << v->size() << numeric_limits<uint32_t>::max();
                }
                uint32_t size = (uint32_t)v->size();
                if (!isBinary)
                {
                    //strip away the terminating null character
                    size = size-1;
                }
                if (fwrite(v->data(), 1, size, f) != size)
                {
                    int err = errno ? errno : EIO;
                    throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER, SCIDB_LE_FILE_WRITE_ERROR) << ::strerror(err) << err;
                }
                ++(*chunkIterators[i]);
                ++i;
            }
            for (size_t i = 0; i < N_ATTRS; i++)
            {
                ++(*arrayIterators[i]);
            }
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
        MemArrayBuilder writer(_schema, query);
        shared_ptr<Array>& input = inputArrays[0];
        shared_ptr< Array> outArray;
        if(settings.isBinaryFormat())
        {
            outArray = convertToBinary(input, settings, writer);
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
        std::set<AttributeID> attributeOrdering;
        for  ( Attributes::const_iterator a = attribs.begin(); a != attribs.end(); ++a )
        {
            if (!a->isEmptyIndicator())
            {
                attributeOrdering.insert(a->getId());
            }
        }
        tmpRedistedInput = pullRedistributeInAttributeOrder(outArray,
                                                            attributeOrdering,
                                                            query,
                                                            psLocalInstance,
                                                            saveInstanceID,
                                                            std::shared_ptr<CoordinateTranslator>(),
                                                            0,
                                                            std::shared_ptr<PartitioningSchemaData>());
        bool const wasConverted = (tmpRedistedInput != outArray) ;
        if (saveInstanceID == myInstanceID)
        {
            saveToDisk(tmpRedistedInput, settings.getFilePath(), query, settings.isBinaryFormat(), false);
        }
        if (wasConverted) {
            SynchableArray* syncArray = safe_dynamic_cast<SynchableArray*>(tmpRedistedInput.get());
            syncArray->sync();
        }
        return shared_ptr<Array>(new MemArray(_schema, query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalAltSave, "alt_save", "PhysicalAltSave");


} // end namespace scidb
