//**********************************************************************`
//* Exabytes.Product                                                   *`
//* src\UnitTests\TestUtils.cpp                                        *`
//* 2015-03-04                                                         *`
//*                                                                    *`
//* Utility functions for Unit Tests                                   *`
//*                                                                    *`
//* Copyright (c) Microsoft Corporation 2015. All Rights Reserved.     *`
//**********************************************************************`

#include "stdafx.h"
#include "UtilitiesWin.hpp"
#include <stdarg.h>

#include "TestUtils.hpp"
#include "ChainedFileStore.hpp"
#include "EbPartition.hpp"
#include "CppUnitTestLogger.h"

namespace EBTest
{

    //
    //  Size of buffer used by 'Trace' functions below.
    //
	#define EBTEST_TRACE_BUFFSIZE 1024

	//
    //	- Utility function to log trace messages to VS output window when
    //	  running unit tests.
    //  - This function calls 'vswprintf_s' with a buffer of size
    //    EBTEST_TRACE_BUFFSIZE. Should the formatted message require a longer
    //    buffer, this function will raise an run-time assertion in Debug mode
    //    and terminate. In Release mode, it will crash.
    //	- Current code is a mix of char/wchar_t, hence both overloads needed
    //	  for now.
    //
	void Trace (
		_In_ const wchar_t* format ,
        _In_ ...                   )
	{
		wchar_t buffer[EBTEST_TRACE_BUFFSIZE];

		va_list vl;
		va_start(vl, format);
		::vswprintf_s(buffer, EBTEST_TRACE_BUFFSIZE, format, vl);
		va_end(vl);

		Microsoft::VisualStudio::CppUnitTestFramework::Logger::WriteMessage(buffer);
	}

	//
    //	- Utility function to log trace messages to VS output window when
    //	  running unit tests.
    //  - This function calls 'vsprintf_s' with a buffer of size
    //    EBTEST_TRACE_BUFFSIZE. Should the formatted message require a longer
    //    buffer, this function will raise an run-time assertion in Debug mode
    //    and terminate. In Release mode, it will crash.
    //	- Current code is a mix of char/wchar_t, hence both overloads needed
    //	  for now.
    //
	void Trace (
		_In_ const char* format ,
        _In_ ...                )
	{
		char buffer[EBTEST_TRACE_BUFFSIZE];

		va_list vl;
		va_start(vl, format);
		::vsprintf_s(buffer, EBTEST_TRACE_BUFFSIZE, format, vl);
		va_end(vl);

		Microsoft::VisualStudio::CppUnitTestFramework::Logger::WriteMessage(buffer);
	}
    
    std::unique_ptr<Exabytes::Partition> 
        TestPartitionCreator::CreatePartition(
        Exabytes::ExabytesServer& server,
        Datagram::PartitionId id,
        intptr_t handle,
        bool isPrimary
        )
    {
        if (!isPrimary)
            Audit::NotImplemented("Secondary partition not supported yet.");

        Schedulers::Scheduler& scheduler = server.GetScheduler();

        // get path for the catalog file
        std::wstring path;
        size_t fileSize;
        auto status = Utilities::GetFileInfoFromHandle(handle, path, fileSize);
        path.append(L"-catalogFile");

        std::unique_ptr<Exabytes::Catalog> pCatalog =
			Exabytes::ReducedMapCatalogFactory(scheduler, path.c_str(), server.GetCatalogBufferPool(), server.GetCatalogBufferPoolForBloomKey(), 0xBAADF00DADD5C731ull);
        unique_ptr<Exabytes::KeyValueOnFile> pSSD = Exabytes::ChainedFileStoreFactory(
            scheduler,
            Exabytes::AddressSpace::SSD,
            handle
            );

        return Exabytes::IsolatedPartitionFactory(server, pCatalog, pSSD, id);
    }
        
    void EBTest::InventoryVector:: push_back(pair<Datagram::PartialDescription, uint32_t> KVPair)
    {
        Utilities::Exclude<SRWLOCK>guard{ m_vectorLock };
        inventory.push_back(KVPair);
    }
    void EBTest::InventoryVector::SetAndReleaseSelf(vector<pair<Datagram::PartialDescription, uint32_t>>& vector)
    {
        Utilities::Exclude<SRWLOCK>guard{ m_vectorLock };
        vector = std::move(inventory);
        inventory.clear();
    }
} //namespace EBTest
