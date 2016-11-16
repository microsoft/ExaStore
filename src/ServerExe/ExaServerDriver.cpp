// ExaServer.cpp : Defines the entry point for the console application.
//

#pragma once
#include "stdafx.h"

#include "Datagram.hpp"
#include "ServiceBrokerRIO.hpp"
#include "Exabytes.hpp"
#include "ReducedKeyMap.hpp"
#include "MemoryStore.hpp"
#include "EbPartition.hpp"
#include "EbServer.hpp"
#include "ChainedFileStore.hpp"

#include "UtilitiesWin.hpp"

#include "Pathcch.h"
#include <memory>
#include <iostream>
#include <vector>
#include <stack>
#include <csignal>
#include <mutex>
#include <condition_variable>
#include <random>

namespace
{
    using namespace Datagram;
    using namespace ServiceBrokers;
    using namespace Schedulers;
    using namespace Exabytes;
    using namespace Utilities;

    volatile sig_atomic_t quit = 0;
    std::mutex exit_lock;
    std::condition_variable exit_cv;

    std::unique_ptr<ExabytesServer> g_pServer;
    void signal_handler(int)
    {

        Tracer::LogInfo(StatusCode::OK, L"Exabyte Server Shutting Down.");
        std::cout << "Exabyte Server Shutting Down..." << std::endl;

        g_pServer->StartCleanup();
        Sleep(2000);
        g_pServer.reset();
        Sleep(1000);
        Tracer::DisposeLogging();

        {
            std::unique_lock<std::mutex> guard(exit_lock);
            quit = 1;
            exit_cv.notify_all();
        }
    }

    static const size_t PARTITION_SIZE_G = 10;
    static const size_t MAX_PARTITIONS = 100;

    class SSDPartitionCreator : public PartitionCreater
    {        
        std::random_device m_seed;
        std::uniform_int_distribution<uint64_t> m_r;

    public:        
        std::unique_ptr<Exabytes::Partition> CreatePartition(
            ExabytesServer& server,
            Datagram::PartitionId id,
            intptr_t fileHandle,
            bool isPrimary
            ) override
        {
            if (!isPrimary)
                Audit::NotImplemented("Secondary partition not supported yet.");

            Scheduler& scheduler = server.GetScheduler();
            uint64_t random = m_r(m_seed);

            // get path for the catalog file
            std::wstring path;
            size_t fileSize;
            auto status = GetFileInfoFromHandle(fileHandle, path, fileSize);
            Audit::Assert((StatusCode)status == StatusCode::OK,
                "Can not get file name from handle!");
            path.erase(path.find('.')).append(L"-catlogFile");
            
            std::unique_ptr<Exabytes::Catalog> pCatalog =
                Exabytes::ReducedMapCatalogFactory(scheduler, path.c_str(), server.GetCatalogBufferPool(), server.GetCatalogBufferPoolForBloomKey(), random);

            std::unique_ptr<Exabytes::KeyValueOnFile> pSSD = Exabytes::ChainedFileStoreFactory(
                scheduler,
                Exabytes::AddressSpace::SSD,
                fileHandle
                );

            return Exabytes::IsolatedPartitionFactory(server,
                pCatalog, pSSD, id);
        }
    };

    class FixExpirePolicyManager : public Exabytes::PolicyManager
    {
        Datagram::Policy m_policy;
    public:
        FixExpirePolicyManager(uint16_t expireInHours)
            : m_policy(expireInHours, false)
        {}

        // Each data owner can have it's own policy, such as
        // expiration time for its data.
        //
        const Datagram::Policy GetPolicy(Datagram::Key64) const override
        {
            return m_policy;
        }

        // Set policy for certain owner
        void SetPolicy(Datagram::Key64, const Datagram::Policy&) override
        {
            // do nothing
        }
    };

    size_t g_numPartitions = 1;
    // every key maps to partition 
    Datagram::PartitionId DummyHash(const Datagram::Key128 key)
    {
        auto res64 = key.ValueHigh() ^ key.ValueLow();
        uint32_t res = (uint32_t)((res64 >> 32) ^ res64);
        res = (res >> 16) ^ res;
        return res % g_numPartitions;
    }

}

int _tmain(int argc, _TCHAR* argv[])
{
    if (argc < 3 || argc % 2 == 0){
        std::wcout << L"Please specify directories for creating data files, and quota." << std::endl;
        std::wcout << L"Usage: TestNative  path  quota_in_GB [path  quota_in_GB]" << std::endl;
        return -1;
    }

    std::stack<std::wstring> fileNames;

    // a directory to store misc files, e.g. memory store back up
    std::wstring miscDir; 

    size_t numPartitions = 0;
    for (int j = 1; j < argc; j += 2){
        wchar_t* path = argv[j];
        size_t quotaInG = _wtoi(argv[j + 1]);
        if (quotaInG == 0){
            std::wcout << L"Please specify directories for creating data files, and quota." << std::endl;
            std::wcout << L"Usage: TestNative  path  quota_in_GB [path  quota_in_GB]" << std::endl;
            return -1;
        }

        wchar_t fileNameBuf[_MAX_PATH + 1];

        size_t numFiles = quotaInG / PARTITION_SIZE_G;
        for (int i = 0; i < numFiles; i++){
            if (numPartitions >= MAX_PARTITIONS)
                break;

            swprintf_s(fileNameBuf, L"ExaPart%04x.dat", i);
            auto res = PathCchCombine(fileNameBuf, _MAX_PATH, path, fileNameBuf);
            if (res != S_OK){
                Audit::OutOfLine::Fail((StatusCode)res);
            }
            fileNames.push(fileNameBuf);
            numPartitions++;
        }
    }

    g_numPartitions = fileNames.size();


    Tracer::InitializeLogging(L"D:\\data\\Exabytes\\Exaserver.etl");
    std::wstring name{ L"home service" };
    Utilities::EnableLargePages();

    //TODO!! change to dynamically allocated port when we figure out how to deal with AP firewall
    std::unique_ptr<ServiceBroker> pSB = ServiceBrokerRIOFactory(name, 52491);
    std::cout << "address " << pSB->HomeAddressIP() << " port " << pSB->HomeAddressPort() << '\n';

    std::unique_ptr<Exabytes::PolicyManager> pPolicies =
        std::make_unique<FixExpirePolicyManager>(24*7);
    std::unique_ptr<PartitionCreater> pPartitionFactory =
        std::make_unique<SSDPartitionCreator>();
    auto pBufferPool = Exabytes::BufferPoolFactory(2 * SI::Mi, 6, Exabytes::MakeFileCoalescingBuffer);
    auto pHashPool = Exabytes::SharedCatalogBufferPoolFactory(INMEMHASHBLKCOUNT, TOTALHASHBLKS, g_numPartitions, HASHBLOCKSIZE);

    // Why this formular?
    // Normal time we only need INMEMHASHBLKCOUNT for each partition, leave 512 for flushing queue 
    // During GC we bring all bloom key blocks to memory
    // We need to leave room for blocks waiting to be flushed after compaction
    auto numBloomKey = (INMEMHASHBLKCOUNT + 512) * (g_numPartitions - 1) + TOTALHASHBLKS + 1024;
    auto pBloomKeyPool = Exabytes::SharedBlockPoolFactory((uint32_t)numBloomKey, BLOOMKEYBLOCKSIZE);
    auto pRecover = Exabytes::CircularLogRecoverFactory();

    g_pServer = Exabytes::IsolatedServerFactory(
        pSB,
        pBufferPool,
        pPolicies,
        pHashPool,
        pBloomKeyPool,
        pPartitionFactory,
        pRecover,
        DummyHash,
        Exabytes::FileStoreGcFactory);

    // TODO!! size of the memory store decided by max partitions
    std::unique_ptr<Exabytes::KeyValueInMemory> pMemoryStore =
        Exabytes::MemoryStoreFactory(*g_pServer, 1024 * SI::Mi);
    g_pServer->SetMemoryStore(pMemoryStore);

    std::cout << "Warning: ADDING DUMMY PARTITIONS FOR TESTING." << std::endl;
    std::cout << "Remove them when manager is ready to schedule partitions" << std::endl;
    for (int i = 0; i < g_numPartitions; i++)
    {
        // TODO!!! Here we only create SSD files, later need to add HDD files
        auto const partitionFileSize = 10 * SI::Gi;        
        static_assert(partitionFileSize == 10 * SI::Gi,
            "Changing partition file size will require changes to catalog capacity calculation. Visit ReducedKeyMap.cpp to reevaluate them.");
        auto res = g_pServer->CreatePartFile(fileNames.top().c_str(), partitionFileSize, true);
        if (res != StatusCode::OK){
            Audit::OutOfLine::Fail(res);
        }

        fileNames.pop();

        res = g_pServer->StartPartition(i, true);
        if (res != StatusCode::OK){
            Audit::OutOfLine::Fail(res);
        }
    }

    g_pServer->StartSweeper();
    g_pServer->StartGC();

    // TODO!! tell the manager we are ready

    // expect first command to be adding a 32 new partitions
    // and then read/write

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
#ifdef SIGBREAK
    signal(SIGBREAK, signal_handler);
#endif

    while (!quit)
    {
        std::unique_lock<std::mutex> guard(exit_lock);
        exit_cv.wait(guard);
    }
	return 0;
}

