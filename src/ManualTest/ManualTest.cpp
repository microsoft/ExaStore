// FixedServerTestClient.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include <iostream>
#include <sstream>
#include <memory>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include "Datagram.hpp"
#include "FixedServerBroker.hpp"

#include <winsock2.h>
#include <Ws2tcpip.h>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>
#include <fstream>
#include <iostream>

namespace {
    using namespace Datagram;
    using namespace Utilities;
    using namespace ExaBroker;

    static const size_t OWNER_KEY = 0xCafeF00d0000ULL;
    static const size_t MAX_RECORDS = 8192;

    std::random_device g_seed;
    std::uniform_int_distribution<uint64_t> g_r;

    class RandomPool
    {
        RandomPool& operator=(const RandomPool&) = delete;
        RandomPool(const RandomPool&) = delete;

        void* const m_pBuf;
        const size_t m_size;

        // Alignment of the start of the record
        // must be power of 2
        static const uint64_t ALIGNMENT = 16;

    public:
        RandomPool(size_t size = 2 * SI::Mi)
            : m_size(size)
            , m_pBuf(new char[size])
        {
            uint64_t* pChar = (uint64_t*)m_pBuf;
            uint64_t* pBeyond = (uint64_t*)((char*)m_pBuf + m_size);
            uint64_t i = 0;
            while (pChar < pBeyond)
            {
                *pChar++ = i++;
            }
        }

        // Generate a random data record.
        //
        void GenerateRandomRecord(size_t size, _Out_ Datagram::Description& descriptor, _Out_ void*& blob)
        {
            Audit::Assert(size > 1 * SI::Ki && size < 1 * SI::Mi, "record size out of range");

            size_t offset = g_r(g_seed);
            offset = offset % (m_size - size);
            offset = offset & ~(ALIGNMENT - 1);

            blob = (char*)m_pBuf + offset;

            ::memset(&descriptor, 0, sizeof(descriptor));

            Datagram::Key128 temp{ g_r(g_seed), g_r(g_seed) };
            descriptor.KeyHash = temp;
            descriptor.ValueLength = (uint32_t)size;
            descriptor.Owner = OWNER_KEY;
            descriptor.Reader = 0;
            descriptor.Part = 0;
        }

        uint64_t GetRandomNumber(){
            return g_r(g_seed);
        }

    };

    RandomPool g_gen;
    std::unique_ptr<ClientBroker> g_pBroker;
    using TestRecord = std::pair < Datagram::Key128, std::pair<void*, size_t> >;

    struct InventoryManagerEntry
    {
        TestRecord m_record;
        bool m_expectedToBeFound;
        bool m_isReadRecord;

        InventoryManagerEntry(TestRecord record, bool readRecord = true, bool presentInServer = true)
            : m_record(record)
            , m_expectedToBeFound(presentInServer)
            , m_isReadRecord(readRecord)
        { }
    };

    // A parallel inventory to enable independent read/write
    class ParallelInventoryManager
    {
    private:
        std::vector<TestRecord> m_records;

        std::mutex lock;
        std::condition_variable not_full;

    public:
        ParallelInventoryManager()
        {
            m_records.reserve(MAX_RECORDS);
        }

        // Adds an entry to the inventory 
        void InsertEntry(const TestRecord& entry)
        {
            // Setting a hard limit of MAX_RECORDS on the inventory size
            // we do not need to send back results of this operation
            std::unique_lock<std::mutex>l(lock);

            if (m_records.size() > MAX_RECORDS)
            {
                Tracer::LogInfo(Utilities::StatusCode::OK, L"Inventory full.");
                not_full.wait(l, [this](){return !(m_records.size() > MAX_RECORDS); });
                Tracer::LogInfo(Utilities::StatusCode::OK, L"Resume Adding to Inventory.");
            }

            m_records.push_back(entry);
        }

        // Get an entry from the inventory
        TestRecord GetEntry(bool remove = false)
        {
            std::unique_lock<std::mutex> l(lock);

            if (m_records.empty())
            {
                return TestRecord{};
            }

            size_t idx = g_r(g_seed) % m_records.size();
            auto iter = m_records.begin() + idx;
            auto ret = *iter;

            if (remove){
                m_records.erase(iter);
                not_full.notify_all();
            }

            return ret;
        }

        // This is for monitoring purpose only. 
        // We log the size periodically to make sure 
        // that the inventory does not grow out of proportion
        // Ideally if the read threads dont starve they will 
        // keep popping the queue and the size wont grow a lot
        size_t GetInventorySize()
        {
            std::unique_lock<std::mutex>l(lock);
            return m_records.size();
        }
    };

    class TestJob {
        TestJob& operator=(const TestJob&) = delete;

        static const int numOfReadThreads = 3;

        std::unordered_map<Datagram::Key128, std::pair<void*, size_t>> m_inventory;

        ParallelInventoryManager m_inserted;
        ParallelInventoryManager m_deleted;

        bool m_continueTests = true;
        std::mutex m_mutex;

        const size_t m_numRecs;
        const bool m_manual;

        void Log(wchar_t* msg, StatusCode error = StatusCode::OK){
            if (m_manual){
                if (error != StatusCode::OK){
                    std::wcerr << msg << " " << (errno_t)error << std::endl;
                }
                else
                {
                    std::wcout << msg  << std::endl;
                }
            }
            else {
                if (error != StatusCode::OK){
                    Tracer::LogError(error, msg);
                }
                else {
                    Tracer::LogInfo(Utilities::StatusCode::OK, msg);
                }
            }
        }
        
        bool Prompt(wchar_t* msg){
            if (m_manual){
                std::wcout << msg << std::endl;
                auto c = std::cin.get();
                return c == 'y' || c == 'Y';
            }
            else {
                Log(msg);
                Sleep(30000);
                return true;
            }
        }

        // Generates a random record of requestd size and issues a single write request
        TestRecord WriteTest(size_t recSize)
        {
            wchar_t msg[1024];
            Description header;
            void* dataParts;
            g_gen.GenerateRandomRecord(recSize, header, dataParts);

            swprintf_s(msg, L"Writing: %llu, data size %d", header.KeyHash.ValueLow(), header.ValueLength);
            Log(msg);

            auto status = g_pBroker->Write(header, dataParts, header.ValueLength);
            if (status != StatusCode::OK)
            {
                Log(L"Write failed ", status);
                header.KeyHash.Invalidate();
                return std::make_pair(header.KeyHash, std::make_pair(nullptr, (size_t)0));
                //Audit::OutOfLine::Fail(status);
            }
            else {
                Log(L"Write successful ");
                return std::make_pair(header.KeyHash, std::make_pair(dataParts, (size_t)header.ValueLength));
            }
        }

        void DumpToFile(std::string path, const char* ptr, size_t size)
        {
            std::ofstream myfile(path);

            const uint64_t* vPtr = reinterpret_cast<const uint64_t*>(ptr);
            for (int i = 0; i < size/sizeof(uint64_t); i++, vPtr++)
            {
                myfile << (*vPtr) << std::endl;
            }
            myfile.close();
        }

        // This issues a read request on server.
        // if this method is being called after deleting the record then the
        // expectedToBeFound boolean should be set to false
        void ReadTest(const TestRecord& entry, bool expectedToBeFound = true)
        {
            PartialDescription keyAndCaller;
            keyAndCaller.KeyHash = entry.first;
            keyAndCaller.Caller = 0;
            DisposableBuffer* pResult = nullptr;
            wchar_t msg[1024];

            swprintf_s(msg, L"Reading: %llu size %zu", keyAndCaller.KeyHash.ValueLow(), entry.second.second);
            Log(msg);
            auto status = g_pBroker->Read(keyAndCaller, &pResult);
            if (expectedToBeFound)
            {
                if (status != StatusCode::OK)
                {
                    Log(L"Read failure", status);
                }
                else
                {
                    bool match = 0 == memcmp(entry.second.first, (char*)pResult->PData() + pResult->Offset(), entry.second.second);
                    if (!match)
                    {
                        swprintf_s(msg, L"Read value mismatch %llu size: %zu", keyAndCaller.KeyHash.ValueLow(), entry.second.second);
                        Log(msg, Utilities::StatusCode::Unexpected);

                        DisposableBuffer* pReRead = nullptr;
                        status = g_pBroker->Read(keyAndCaller, &pReRead);
                        if (status != StatusCode::OK)
                        {
                            Audit::OutOfLine::Fail(status, "Retry Read failed after mismatched result.");
                        }
                        match = 0 == memcmp(entry.second.first, (char*)pReRead->PData() + pReRead->Offset(), entry.second.second);

                        DumpToFile("D:\\Data\\ExaBroker\\expected.txt", (char*)entry.second.first, entry.second.second);
                        DumpToFile("D:\\Data\\ExaBroker\\val1.txt", (char*)(char*)pResult->PData() + pResult->Offset(), pResult->Size() - pResult->Offset());
                        DumpToFile("D:\\Data\\ExaBroker\\val2.txt", (char*)(char*)pReRead->PData() + pReRead->Offset(), pReRead->Size() - pReRead->Offset());

                        if (match)
                        {
                            Log(L"Reread success!", Utilities::StatusCode::Unexpected);
                            Audit::OutOfLine::Fail(status, "Read result mismatch, reread success.");
                        }
                        else {
                            Log(L"Reread mismatch again!", Utilities::StatusCode::Unexpected);
                            Audit::OutOfLine::Fail(status, "Read result mismatch, reread mismatch!");
                        }
                        pReRead->Dispose();
                        pReRead = nullptr;
                    }
                    else
                    {
                        Log(L"Read value verification successful. ");
                    }
                }
            }
            else
            {
                if (status != StatusCode::NotFound && status != StatusCode::Deleted)
                {
                    Log(L"read of deleted record get unexpected result: ", status);
                }
                else
                {
                    Log(L"Deleted record not found, success.");
                }
            }
            if (pResult != nullptr){
                pResult->Dispose();
                pResult = nullptr;
            }            
        }

        // This issues a single delete request
        bool DeleteTest(const TestRecord& entry)
        {
            wchar_t msg[1024];
            PartialDescription keyAndCaller;
            keyAndCaller.KeyHash = entry.first;
            keyAndCaller.Caller = OWNER_KEY;

            swprintf_s(msg, L"Deleting: %llu", keyAndCaller.KeyHash.ValueLow());
            Log(msg);

            auto status = g_pBroker->Delete(keyAndCaller);
            if (status != StatusCode::OK)
            {
                Log(L"Delete failed ", status);
                return false;
            }
            else
            {
                Log(L"Delete successful ");;
                return true;
            }
        }

        // This is executed in write thread;
        // The thread will stop only when the m_continueTests
        // is set to false;
        // BeginWrite will generate random records and request writes to EBServer
        void BeginWrite()
        {
            int count = 0;
            do
            {
                auto record = WriteTest(150000);
                if (!record.first.IsInvalid())
                {
                    m_inserted.InsertEntry(record);
                }
                    
                record = WriteTest(2000);
                if (!record.first.IsInvalid())
                {
                    m_inserted.InsertEntry(record);
                }

                auto size = m_inserted.GetInventorySize();

                count++;
                if (count == 64)
                {
                    // This only for monitoring purpose to make sure the 
                    // inventory does not grow out of proportion
                    // we will also use this info to tune the read and write threads
                    std::wstringstream msg;
                    msg << L"InventorySize: " << size;
                    Log(const_cast<wchar_t*>(msg.str().c_str()));
                    count = 0;
                }
                Sleep(50);

            } while (m_continueTests);
        }

        void BeginDelete()
        {
            do
            {
                Sleep(128);
                auto size = m_inserted.GetInventorySize();

                if (size > (MAX_RECORDS / 2)){
                    auto del = m_inserted.GetEntry(true);
                    if (del.second.first == nullptr)
                    {
                        continue;
                    }

                    if (DeleteTest(del)){
                        m_deleted.InsertEntry(del);
                        if (m_deleted.GetInventorySize() > MAX_RECORDS / 2)
                        {
                            m_deleted.GetEntry(true);
                        }
                    }
                }
            } while (m_continueTests);

        }

        // This is executed in read thread;
        // The thread will stop only when the m_continueTests
        // is set to false;
        void BeginRead()
        {
            do
            {
                // Fetch an entry from the read queue as soon as it is available
                auto entry = m_inserted.GetEntry();
                if (entry.second.first == nullptr)
                {
                    Log(L"Invetory Empty when reading");
                    Sleep(1000);
                    continue;
                }
                ReadTest(entry);

                auto del = m_deleted.GetEntry();
                if (del.second.first == nullptr)
                {
                    continue;
                }
                ReadTest(entry, false);

            } while (m_continueTests);
        }

        // This starts a serial tests
        // This test creates sequential write \read\delete\read requests
        void TestOnce()
        {
            // Start write requests
            // add each record to the inventory so that we can read\delete it later
            for (int i = 0; i < m_numRecs; i++){
                auto entry = WriteTest(200000);
                if (!entry.first.IsInvalid())
                    m_inventory.insert(entry);

                entry = WriteTest(10000);
                if (!entry.first.IsInvalid())
                    m_inventory.insert(entry);
            }

            // 
            Prompt(L"Press any key to read...");
            // Read all the written records
            for (auto& x : m_inventory)
            {
                ReadTest(TestRecord(x));
            }

            // Delete all the records present in inventory
            auto iter = m_inventory.begin();
            if (iter == m_inventory.end()){
                Log(L"Nothing to delete");
            }
            else {
                for (auto& record : m_inventory)
                {
                    DeleteTest(TestRecord(record));
                }
            }

            // read them back and make sure we dont find them on the server
            Prompt(L"Press any key to read the deleted...");
            for (auto& record : m_inventory)
            {
                ReadTest(static_cast<TestRecord>(record), false);
            }
            m_inventory.clear();
        }

    public:
        TestJob(bool manual, size_t numRecords)
            : m_numRecs(numRecords)
            , m_manual(manual)
        { }
       
        void StartSerialTests()
        {
            for (;;){
                TestOnce();

                if (!Prompt(L"Press Y to start next batch..."))
                    break;
            }
        }

        void StartConcurrentTests()
        {
            // start writer thread
            std::thread writer([this](){
                BeginWrite();
            });

            // start delete thread
            std::thread deleter([this](){
                BeginDelete();
            });

            // start read and delete threads
            std::vector<std::thread> readThreads;
            for (int i = 0; i < numOfReadThreads; i++)
            {
                readThreads.push_back(std::thread([this](){
                    BeginRead();
                }));
            }

            // wait untill user prmopts to end the test
            // in case of auto : this will wait forever
            while (Prompt(L"Press Y to start next batch..."));
                
            // set m_continueTest to false. This will indicate
            // all the running threads to exit
            {
                std::lock_guard<std::mutex> guard(m_mutex);
                m_continueTests = false;
            }

            // wait for all the threads
            writer.join();
            deleter.join();
            for (auto &readAndDeleteThread : readThreads)
            {
                readAndDeleteThread.join();
            }
        }
    };
};

int _tmain(int argc, _TCHAR* argv[])
{
    bool serial = true;
    bool manual = true;
    size_t num_in_batch = 1;

    if (argc < 3)
    {
        std::cout << "Usage: ManualTest <server ip> <port> <manual/auto> <num in a batch> <serial/concurrent>" << std::endl;
        return -1;
    }
    
    sockaddr_in server;
    server.sin_family = AF_INET;
    InetPton(AF_INET, argv[1], &server.sin_addr);
    server.sin_port = htons(static_cast<uint16_t>(_wtoi(argv[2])));

    std::wcout << L"Client: " << argv[1] << L":" << argv[2] << std::endl;

    if (argc >= 4){
        manual = (wcscmp(L"auto", argv[3]) != 0);
        if (manual){
            std::cout << "Mode: auto" << std::endl;
        }
        else {
            std::cout << "Mode: manual" << std::endl;
        }
    }

    if (argc >= 5){
        num_in_batch = _wtoi(argv[4]);
        std::cout << "Batch size: " << num_in_batch << std::endl;
    }

    if (argc >= 6){
        serial = (wcscmp(L"serial", argv[5]) == 0);
    }
    
    g_pBroker = ExaBroker::TestBrokerFactory(ntohl(server.sin_addr.S_un.S_addr), ntohs(server.sin_port));

    std::cout << "broker created..." << std::endl;

    TestJob job(manual, num_in_batch);
    if (serial){
        job.StartSerialTests();
    }
    else{
        job.StartConcurrentTests();
    }

    std::cout << "test finished ..." << std::endl;

    return 0;
}

