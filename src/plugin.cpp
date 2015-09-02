/*
 **
 * BEGIN_COPYRIGHT
 *
 * PARADIGM4 INC.
 * This file is part of the Paradigm4 Enterprise SciDB distribution kit
 * and may only be used with a valid Paradigm4 contract and in accord
 * with the terms and conditions specified by that contract.
 *
 * Copyright (C) 2010 - 2015 Paradigm4 Inc.
 * All Rights Reserved.
 *
 * END_COPYRIGHT
 */

#include <vector>

#include <SciDBAPI.h>
#include <system/ErrorsLibrary.h>

using namespace scidb;

EXPORTED_FUNCTION void GetPluginVersion(uint32_t& major, uint32_t& minor, uint32_t& patch, uint32_t& build)
{
    major = SCIDB_VERSION_MAJOR();
    minor = SCIDB_VERSION_MINOR();
    patch = SCIDB_VERSION_PATCH();
    build = SCIDB_VERSION_BUILD();
}

class Instance
{
public:
    Instance()
    {}

    ~Instance()
    {}

} _instance;
