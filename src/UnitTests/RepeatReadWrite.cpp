#pragma once
#include "stdafx.h"
#include "CppUnitTest.h"

#include "TestUtils.hpp"

#include "UtilitiesWin.hpp"
#include <algorithm>


namespace EBTest
{
    using namespace Datagram;
    using namespace Exabytes;
    using namespace Schedulers;
    using namespace Utilities;
    using namespace std;

    using namespace Microsoft::VisualStudio::CppUnitTestFramework;

    const bool verbose = false;

    // every key maps to a single partition: 0
    Datagram::PartitionId DummyHash(const Datagram::Key128 key)
    {
        return 0;
    }

    // every key maps to partition 0 or 1
    Datagram::PartitionId BinHash(const Datagram::Key128 key)
    {
        auto res64 = key.ValueHigh() ^ key.ValueLow();
        uint32_t res = (uint32_t)((res64 >> 32) ^ res64);
        res = (res >> 16) ^ res;
        return res & 1;
    }    

    // A RepeatedWriteContinuation is called when the Write has been committed, and should finish the request.
    //
    class RepeatedWriteContinuation : public ContinuationBase
    {
        bool*             m_pStopActions;
        long*             m_pActionCount;
        Description*      m_pDesc;
        pair<Datagram::PartialDescription, uint32_t> m_data;
        vector<pair<Datagram::PartialDescription, uint32_t>>* m_pInv;
    public:
        RepeatedWriteContinuation(
            _In_ Activity&         activity,
            Description*           pDescription,
            _Inout_ long*          pActionCount,
            _Inout_ bool*          pStopActions,            
            _Inout_ pair<Datagram::PartialDescription, uint32_t>* pData,
            _Inout_ vector<pair<Datagram::PartialDescription, uint32_t>>* pInv            
            )
            : ContinuationBase(activity)
            , m_pDesc(pDescription)
            , m_pActionCount(pActionCount)
            , m_pStopActions(pStopActions)
            , m_data(*pData)
            , m_pInv(pInv)
        {}

        Description* GetWriteRec()
        {
            return m_pDesc;
        }

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            )
        {                        
            if ((intptr_t)E_OUTOFMEMORY == continuationHandle)
            {               
               // wipe out the crc for this record to indicate the read test that
               // this record is not on the server and that the expected read
               // result for this record should be NotFound               
               auto itr = std::find_if(m_pInv->begin(), m_pInv->end(), [&](pair<Datagram::PartialDescription, uint32_t>const& data)
               { return m_data.first.KeyHash == data.first.KeyHash; });

               if (itr != m_pInv->end())
               {
                   itr->second = 0;
               }

               // this indicates to stop further writes
               *m_pStopActions = true;
               
            }
            else if ((intptr_t)S_OK != continuationHandle)
            {
                Audit::OutOfLine::Fail((StatusCode)continuationHandle, "failed write operation");
            }

            if (verbose)
            {
				EBTest::Trace("Finished Writing %d", m_pDesc->KeyHash.ValueLow());
            }

            free(m_pDesc);
            m_activity.RequestShutdown(StatusCode::OK);
            // send a confirmation back to the writer
            int newValue = ::InterlockedDecrement(m_pActionCount);
        }

        void Cleanup() override
        {}
    };

    // This continuation is to process the write request to mem store
    // After it finishes the write it posts a continuation to end activity and reclaim arena
    class MemStoreWriteContinuation : public ContinuationBase
    {
    private:
        KeyValueInMemory*                       m_pStore;
        Scheduler*                              m_pScheduler;
        RandomWrites                            m_writes;
        long*                                   m_pActionCount;
        shared_ptr<InventoryVector> m_pInventory;

    public:
        MemStoreWriteContinuation(KeyValueInMemory& store, Scheduler& scheduler, Activity& activity, _Inout_ long* writesincomplete, shared_ptr<InventoryVector> pInventory)
            : ContinuationBase(activity)
            , m_pStore(&store)
            , m_pScheduler(&scheduler)
            , m_pActionCount(writesincomplete)
            , m_pInventory(pInventory)
        {        }

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            Description header;
            void* dataParts[1];
            uint32_t dataSizes[1];
            m_writes.GenerateRandomRecord(header, dataParts[0]);
            dataSizes[0] = header.ValueLength;
            header.Timestamp = m_pScheduler->GetCurTimeStamp();

            if (verbose)
            {
                Trace("Dispatch Writing %d", header.KeyHash.ValueLow());
            }

            // allocate memory for write request
            // will be freed at the end of this continuation
            Description* pWriteReq = reinterpret_cast<Description*>(malloc(sizeof(Description) + dataSizes[0]));
            *pWriteReq = header;
            void* pBlob = (char*)pWriteReq + sizeof(Description);
            memcpy_s(pBlob, dataSizes[0], dataParts[0], dataSizes[0]);

            auto success = m_pStore->Write(*pWriteReq, pBlob, pWriteReq->ValueLength);                   
            Audit::Assert(success == StatusCode::OK || success == StatusCode::OutOfMemory,
                "Failed to write to memory store");
            if (success == StatusCode::OK)
            {
                auto crc = Utilities::PsuedoCRC(pBlob, pWriteReq->ValueLength);
                m_pInventory->push_back(pair<Datagram::PartialDescription, uint32_t>{
                    { pWriteReq->KeyHash, pWriteReq->Owner }, crc });

                if (verbose)
                {
                    EBTest::Trace("Finished Writing %d", pWriteReq->KeyHash.ValueLow());
                }
            }
                                                                                                   
            free(pWriteReq);
            // send a confirmation back to the writer
            int newValue = ::InterlockedDecrement(m_pActionCount);
            m_activity.RequestShutdown(StatusCode::OK);                        
        }
        void Cleanup() override
        {   }
    };

    class RepeatedMemStoreWrite : public RepeatedAction
    {
    private:
        KeyValueInMemory*    m_pStore;
        Scheduler*           m_pScheduler;
        RandomWrites         m_writes;
        long                 m_writeRequestsRemaining;
        long                 m_writesIncomplete;
        Activity*            m_pWriteActivity;
        shared_ptr<InventoryVector> m_pInventory;

    public:
        RepeatedMemStoreWrite(KeyValueInMemory& store, Scheduler& scheduler, shared_ptr<InventoryVector> pInventory)
            : m_pStore(&store)
            , m_pScheduler(&scheduler)
        {
            m_pInventory = pInventory;
            m_pWriteActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Write Random");            
        }

        bool WriteOneMore()
        {
            // Construct write call back (continuation) with its own context (activity)
            // Do not need to worry about memory leak
            // Activity and continuation are all allocated on an Arena, which will be reclaimed when the activity shuts down
            // 
            auto pActivity = ActivityFactory(*m_pScheduler, L"Write req");
            auto pArena = pActivity->GetArena();
            auto pContinuation = pArena->allocate<MemStoreWriteContinuation>(
                *m_pStore,*m_pScheduler, *pActivity, &m_writesIncomplete, m_pInventory);
                        
            // Posting of pContinuation will start pWriteActivity. Finishing of pContinuation
            // will shutdown pWriteActivity and reclaim the arena
            //
            pContinuation->Post((intptr_t)this, 0, 0);
            
            --m_writeRequestsRemaining;

            return 0 < m_writeRequestsRemaining;
        }

        static bool WriteAgainFunc(RepeatedAction& testGen)
        {
            return ((RepeatedMemStoreWrite*)(&testGen))->WriteOneMore();
        }

        void Run(int count) override
        {
            //  create some blobs, write to store.
            m_writeRequestsRemaining = count;
            m_writesIncomplete = count;

			RunAgainActor* writeActor = m_pWriteActivity->GetArena()->allocate<RunAgainActor>(*m_pWriteActivity, *this, &WriteAgainFunc);
            writeActor->Post((intptr_t) this, 0, 0);

            int seconds = 0;
            while (0 < m_writesIncomplete)
            {
                //Audit::Assert(seconds < count, "the test timed out for Write");
				Trace("Write count remaining %d", m_writeRequestsRemaining);
                Sleep(1000);
                ++seconds;
            }

            writeActor->m_activity.RequestShutdown(StatusCode::OK, L"Test Completed");
        }
    };

    std::unique_ptr<RepeatedAction> RepeatedMemStoreWriteFactory(
        _In_ Exabytes::KeyValueInMemory& store,
        _In_ Schedulers::Scheduler& scheduler,
        shared_ptr<InventoryVector> inventory
        )
    {
        return make_unique<RepeatedMemStoreWrite>(store, scheduler, inventory);
    }

    // Repeated write multi-parts records to a store.
    // this is code clone with RepeatedMemStoreWrite with minor change. since this is test code
    // I am gonna stop worrying about it.
    // 
    class RepeatedMultiWrite : public RepeatedAction
    {
    private:
        ExabytesServer*      m_pServer;
        Scheduler*           m_pScheduler;
        RandomWrites         m_writes;
        long                 m_writeRequestsRemaining;
        long                 m_writesIncomplete;
        bool                 m_stopWrites;
        Activity*            m_pWriteActivity;
        vector<pair<Datagram::PartialDescription, uint32_t>>* m_pInventory;

        // ramdom generator to divid a blob into multiple parts
        random_device m_seed;
        uniform_int_distribution<uint64_t> m_r;

    public:
        RepeatedMultiWrite(ExabytesServer& server, Scheduler& scheduler, vector<pair<Datagram::PartialDescription, uint32_t>>& inventory)
            : m_pServer(&server)
            , m_pScheduler(&scheduler)
            , m_pInventory(&inventory)
        {
            m_pWriteActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Write Random");
        }

        // generate a blob, divide into 4 parts and write to the store.
        // Our interface does not garrantee sequence, however this test
        // depends on our implementation, that when writes are in sequence,
        // the results are concatnated in sequence.
        //
        bool WriteOneMore()
        {
            Description header[4];
            void* dataParts[4];
            uint32_t dataSizes[4];
            m_writes.GenerateRandomRecord(header[0], dataParts[0]);
            uint32_t totalBlobLen = header[0].ValueLength;
            header[0].Part = 1;

            auto pPartiton = m_pServer->FindPartitionForKey(header[0].KeyHash);

            auto crc = Utilities::PsuedoCRC(dataParts[0], totalBlobLen);
            auto dataItem = pair < Datagram::PartialDescription, uint32_t > { { header[0].KeyHash, header[0].Owner }, crc};

            for (int i = 0; i < 4; i++)
            {
                if (i < 3)
                {
                    // divide the blob into 4 parts, we need 3 cuts
                    uint32_t divLen = (uint32_t) m_r(m_seed);
                    divLen = divLen % (header[i].ValueLength);
                    uint32_t remainLen = header[i].ValueLength - divLen;

                    header[i + 1] = header[i];
                    header[i].ValueLength = divLen;
                    header[i + 1].ValueLength = remainLen;
                    dataParts[i + 1] = (char*)dataParts[i] + divLen;
                }
                dataSizes[i] = header[i].ValueLength;

                if (verbose)
                {
					Trace("Dispatch multi-Writing %d part %d", header[0].KeyHash.ValueLow(), i);
                }

                // allocate memory for write request
                // will be freed in write continuation
                Description* pWriteReq = reinterpret_cast<Description*>(malloc(sizeof(Description)+dataSizes[i]));
                *pWriteReq = header[i];
                void* pBlob = (char*)pWriteReq + sizeof(Description);
                memcpy_s(pBlob, dataSizes[i], dataParts[i], dataSizes[i]);


                // Construct write call back (continuation) with its own context (activity)
                // Do not need to worry about memory leak
                // Activity and continuation are all allocated on an Arena, which will be reclaimed when the activity shuts down
                // 
                auto pActivity = ActivityFactory(*m_pScheduler, L"Write req");
                auto pArena = pActivity->GetArena();
                auto pContinuation = pArena->allocate<RepeatedWriteContinuation>(*pActivity, pWriteReq, &m_writesIncomplete, &m_stopWrites, &dataItem, m_pInventory);

                // Posting of pContinuation will start pWriteActivity. Finishing of pContinuation
                // will shutdown pWriteActivity and reclaim the arena
                //
                // This is not the right model. write operation and write continuation should be in the same activity
                // 
                Audit::Assert(StatusCode::OK == pPartiton->Write(*pWriteReq, pBlob, dataSizes[i], pContinuation), "KeyValueStore->Write");
            }
            
            m_pInventory->push_back(dataItem);

            --m_writeRequestsRemaining;
            return (0 < m_writeRequestsRemaining && m_stopWrites == false);
        }

        static bool WriteAgainFunc(RepeatedAction& testGen)
        {
            return ((RepeatedMultiWrite*)(&testGen))->WriteOneMore();
        }

        void Run(int count) override
        {
            //  create some blobs, write to store.
            m_writeRequestsRemaining = count;
            m_writesIncomplete = count * 4; // one blob is divided into 4 parts
            m_stopWrites = false;

			RunAgainActor* writeActor = m_pWriteActivity->GetArena()->allocate<RunAgainActor>(*m_pWriteActivity, *this, &WriteAgainFunc);
            writeActor->Post((intptr_t) this, 0, 0);

            int seconds = 0;
            while (0 < m_writesIncomplete && !m_stopWrites)
            {
				Trace("Write count remaining %d", m_writeRequestsRemaining);
                Sleep(1000);
                ++seconds;
            }
        }
    };

    std::unique_ptr<RepeatedAction> RepeatedMultiWriteFactory(
        _In_ Exabytes::ExabytesServer& server, 
        _In_ Schedulers::Scheduler& scheduler,
        _Inout_ vector<pair<Datagram::PartialDescription, uint32_t>>& inventory)
    {
        return make_unique<RepeatedMultiWrite>(server, scheduler, inventory);
    }

    // End to end test driver of single server
    // repeated write to EbServer, mixing single
    // write with multi-part writes
    //
    class RepeatedWrite : public RepeatedAction
    {
    private:
        ExabytesServer*      m_pServer;
        Scheduler*           m_pScheduler;
        RandomWrites         m_writes;
        long                 m_writeRequestsRemaining;
        long                 m_writesIncomplete;
        bool                 m_stopWrites;
        Activity*            m_pWriteActivity;
        vector<pair<Datagram::PartialDescription, uint32_t>>* m_pInventory;
        const bool m_multiParts;

        // ramdom generator to divid a blob into multiple parts
        random_device m_seed;
        uniform_int_distribution<uint64_t> m_r;

    public:
        RepeatedWrite(ExabytesServer& store, Scheduler& scheduler, vector<pair<Datagram::PartialDescription, uint32_t>>& inventory, bool withMultParts = false)
            : m_pServer(&store)
            , m_pScheduler(&scheduler)
            , m_pInventory(&inventory)
            , m_multiParts(withMultParts)
        {
            m_pWriteActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Write Random");
        }

        bool WriteOneMore()
        {
            int numParts = 1;
            if (m_multiParts && (m_writeRequestsRemaining % 32 == 27) )
                numParts = 4;
            Description header[4];
            void* dataParts[4];
            uint32_t dataSizes[4];
            m_writes.GenerateRandomRecord(header[0], dataParts[0]);
            uint32_t totalBlobLen = header[0].ValueLength;

            header[0].Part = numParts == 1 ? 0 : 1;
            ::InterlockedAdd(&m_writesIncomplete, numParts - 1);

            auto pPartiton = m_pServer->FindPartitionForKey(header[0].KeyHash);
            
            auto crc = Utilities::PsuedoCRC(dataParts[0], totalBlobLen);
            auto data = pair < Datagram::PartialDescription, uint32_t >{ { header[0].KeyHash, header[0].Owner }, crc };

            for (int i = 0; i < numParts; i++)
            {
                if (i < numParts-1)
                {
                    // divide the blob into 4 parts, we need 3 cuts
                    uint32_t divLen = (uint32_t) m_r(m_seed);
                    divLen = divLen % (header[i].ValueLength);
                    uint32_t remainLen = header[i].ValueLength - divLen;

                    header[i + 1] = header[i];
                    header[i].ValueLength = divLen;
                    header[i + 1].ValueLength = remainLen;
                    dataParts[i + 1] = (char*)dataParts[i] + divLen;
                }
                dataSizes[i] = header[i].ValueLength;

                if (verbose)
                {
					Trace("Dispatch multi-Writing %d part %d", header[0].KeyHash.ValueLow(), i);
                }

                // Construct write call back (continuation) with its own context (activity)
                // Do not need to worry about memory leak
                // Activity and continuation are all allocated on an Arena, which will be reclaimed when the activity shuts down
                // 
                auto pActivity = ActivityFactory(*m_pScheduler, L"Write req");

                // unlike mem store write, partition will take the data and copy it into a write queue
                // so we don't need to keep the data in the activity context.
                // here this is just let the write continuation know about the header
                //
				Description* pHeader = new Description{ header[i] };

                auto pArena = pActivity->GetArena();
                auto pContinuation = 
                    pArena->allocate<RepeatedWriteContinuation>(*pActivity, pHeader, &m_writesIncomplete, &m_stopWrites, &data, m_pInventory);

                Audit::Assert(
                    StatusCode::OK == pPartiton->Write(header[i], dataParts[i], dataSizes[i], pContinuation),
                    "KeyValueStore->Write");
            }
            
            m_pInventory->push_back(data);

            --m_writeRequestsRemaining;
            return (0 < m_writeRequestsRemaining && m_stopWrites == false);
        }

        static bool WriteAgainFunc(RepeatedAction& testGen)
        {
            return ((RepeatedWrite*)(&testGen))->WriteOneMore();
        }

        void Run(int count) override
        {
            //  create some blobs, write to store.
            m_writeRequestsRemaining = count;
            m_writesIncomplete = count;
            m_stopWrites = false;

			RunAgainActor* writeActor = m_pWriteActivity->GetArena()->allocate<RunAgainActor>(*m_pWriteActivity, *this, &WriteAgainFunc);
            writeActor->Post((intptr_t) this, 0, 0);

            int seconds = 0;
            while (0 < m_writesIncomplete)
            {
                //Audit::Assert(seconds < count, "the test timed out for Write");
				EBTest::Trace("Write count remaining %d", m_writeRequestsRemaining);
                Sleep(1000);
                ++seconds;
                if (m_stopWrites && m_writesIncomplete == m_writeRequestsRemaining)
                {
                    break;
                }
            }
        }
    };

    std::unique_ptr<RepeatedAction> RepeatedWriteFactory(
        _In_ Exabytes::ExabytesServer& server,
        _In_ Schedulers::Scheduler& scheduler,
        _Inout_ vector<pair<Datagram::PartialDescription, uint32_t>>& inventory,
        bool allowMultiParts
        )
    {
        return make_unique<RepeatedWrite>(server, scheduler, inventory, allowMultiParts);
    }

    // A RepeatedReadContinuation is called when the Read has obtained the data ready to be
    // copied to the waiting reader.
    //
    class RepeatedReadContinuation : public Continuation<BufferEnvelope>
    {
        Partition*          m_pPartition;
        PartialDescription  m_partialDescription;
        uint32_t            m_crc;
        long*               m_pActionCount;
    public:
        RepeatedReadContinuation(
            _In_ Activity& activity,
            _In_ Partition* pStore,
            _In_ PartialDescription& partialDescription,
            _In_ uint32_t crc,
            _Inout_ long* pActionCount
            )
            : Continuation(activity)
            , m_pPartition(pStore)
            , m_partialDescription(partialDescription)
            , m_crc(crc)
            , m_pActionCount(pActionCount)
        {}

        PartialDescription* PDescription() { return &(m_partialDescription); }

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            if (m_crc == 0)
            {
                auto result = (StatusCode)continuationHandle;
                // if we see the record with valuelength==0 then status will be deleted
                // if this record was GCed then the status will be NotFound
                Audit::Assert(result == StatusCode::Deleted || result == StatusCode::NotFound, "deleted item found!");
            }
            else
            {                
                Audit::Assert((void*)continuationHandle == (void*)&m_postable, "buffer containing data expected.");
                auto pBuffer = malloc(length);
                m_pPartition->Collect(continuationHandle, length, pBuffer);
                auto crc = Utilities::PsuedoCRC(pBuffer, length);
                free(pBuffer);
                if (m_crc != crc)
                {
                    Audit::Assert(false, "CRC did not match after collection");
                }
            }
            // send a confirmation back to the reader.
            int newValue = ::InterlockedDecrement(m_pActionCount);

            if (verbose)
			{
				Trace("Finished Reading %d", m_partialDescription.KeyHash.ValueLow());
            }

            // tbd: how do we guarantee shutdown?
            // exception handling should use the action and the arena as an image dump
            //  before the arena is retired.
            m_activity.RequestShutdown(StatusCode::OK);
        }

        void Cleanup() override
        {}
    };
    // the Test::QuickGenerator will create a mix of read and write requests where the writes
    //  contain randomized content.  The writes also have a random key, where the key
    //  and a CRC for the writes is recorded.  These are then used to start reading back
    //  prior data and verify that the CRCs match.
    //
    class RepeatedRead : public RepeatedAction
    {
        ExabytesServer*      m_pServer;
        Scheduler*           m_pScheduler;
        Activity*            m_pReadActivity;
        long                 m_readRequestsRemaining;
        long                 m_readsIncomplete;
        vector<pair<Datagram::PartialDescription, uint32_t>>* m_pInventory;

        random_device                      m_seed;
        uniform_int_distribution<uint64_t> m_r;

    public:
        RepeatedRead(ExabytesServer& server, Scheduler& scheduler,
                     vector<pair<Datagram::PartialDescription, uint32_t>>& inventory)
            : m_pServer(&server)
            , m_pScheduler(&scheduler)
            , m_pInventory(&inventory)
        {
            m_pReadActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Read Random");
        }

        // Generate each read on a separate Turn
        //
        bool ReadOneMore()
        {

            int i = (int)(m_r(m_seed) % m_pInventory->size());
            auto pair = (*m_pInventory)[i];
            Datagram::PartialDescription descriptor;
            ::ZeroMemory(&descriptor, sizeof(descriptor));
            descriptor.KeyHash = pair.first.KeyHash;
            auto crc = pair.second;

            if (verbose)
            {
				Trace("Dispatch Reading %d", descriptor.KeyHash.ValueLow());
            }

            // Construct read call back with its own context
            auto pReadActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Read Req");
            auto pArena = pReadActivity->GetArena();
            auto pPartition = m_pServer->FindPartitionForKey(descriptor.KeyHash);
            auto pContinuation = pArena->allocate<RepeatedReadContinuation>(*pReadActivity, pPartition, descriptor, crc, &m_readsIncomplete);

            // issue read
            Audit::Assert(StatusCode::OK == pPartition->Read(100, descriptor, pContinuation), "KeyValueStore->Read");
            // in the completion   auto crc = Utilities::PsuedoCRC(pBuf->PBuffer(), length);

            --m_readRequestsRemaining;
            return 0 < m_readRequestsRemaining;
        }

        static bool ReadAgainFunc(RepeatedAction& testGen)
        {
            return ((RepeatedRead*)(&testGen))->ReadOneMore();
        }

        void Run(int count) override
        {
            m_readRequestsRemaining = count;
            m_readsIncomplete = count;
			RunAgainActor* readActor = m_pReadActivity->GetArena()->allocate<RunAgainActor>(*m_pReadActivity, *this, &ReadAgainFunc);
            readActor->Post((intptr_t) this, 0, 0);

            uint32_t seconds = 0;
            while (0 < m_readsIncomplete)
            {
                //Audit::Assert(seconds < count, "the test timed out for Read");
                EBTest::Trace("Read count remaining %d", m_readRequestsRemaining);
                Sleep(1000);
                ++seconds;
            }
        }
    };

    std::unique_ptr<RepeatedAction> RepeatedReadFactory(ExabytesServer& server, Schedulers::Scheduler& scheduler, vector<pair<Datagram::PartialDescription, uint32_t>>& inventory)
    {
        return make_unique<RepeatedRead>(server, scheduler, inventory);
    }


    // A RepeatedDeleteContinuation is called when the Delete operation finishes
    //
    class RepeatedDeleteContinuation : public ContinuationBase
    {
        Partition*          m_pPartition;
        PartialDescription  m_partialDescription;
        long*               m_pActionCount;
    public:
        RepeatedDeleteContinuation(
            _In_ Activity& activity,
            _In_ Partition* pStore,
            _In_ PartialDescription& partialDescription,
            _Inout_ long* pActionCount
            )
            : ContinuationBase(activity)
            , m_pPartition(pStore)
            , m_partialDescription(partialDescription)
            , m_pActionCount(pActionCount)
        {}

        PartialDescription* PDescription() { return &(m_partialDescription); }

        void OnReady(
            _In_ WorkerThread&  thread,
            _In_ intptr_t       continuationHandle,
            _In_ uint32_t       length
            ) override
        {
            auto result = (StatusCode)continuationHandle;
            Audit::Assert(result == StatusCode::OK || result == StatusCode::NotFound, "delete failed.");
            // send a confirmation back to the reader.
            int newValue = ::InterlockedDecrement(m_pActionCount);

            if (verbose)
            {
                Trace("Finished Deleting %d", m_partialDescription.KeyHash.ValueLow());
            }

            // tbd: how do we guarantee shutdown?
            // exception handling should use the action and the arena as an image dump
            //  before the arena is retired.
            m_activity.RequestShutdown(StatusCode::OK);
        }

        void Cleanup() override
        {}
    };
    class RepeatedDelete : public RepeatedAction
    {
        ExabytesServer*      m_pServer;
        Scheduler*           m_pScheduler;
        Activity*            m_pDeleteActivity;
        long                 m_requestsRemaining;
        long                 m_deleteIncomplete;
        vector<pair<Datagram::PartialDescription, uint32_t>>* m_pInventory;

        random_device                      m_seed;
        uniform_int_distribution<uint64_t> m_r;

    public:
        RepeatedDelete(ExabytesServer& server, Scheduler& scheduler,
            vector<pair<Datagram::PartialDescription, uint32_t>>& inventory)
            : m_pServer(&server)
            , m_pScheduler(&scheduler)
            , m_pInventory(&inventory)
        {
            m_pDeleteActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Delete Random");
        }

        // Generate each delete on a separate Turn
        //
        bool DeleteOneMore()
        {
            int i = (int)(m_r(m_seed) % m_pInventory->size());
            auto pair = (*m_pInventory)[i];
            Datagram::PartialDescription descriptor;
            ::ZeroMemory(&descriptor, sizeof(descriptor));
            descriptor = pair.first;
            pair.second = 0; // clear out the crc
            (*m_pInventory)[i] = pair;

            if (verbose)
            {
                Trace("Dispatch Deleting %d", descriptor.KeyHash.ValueLow());
            }

            // Construct delete call back with its own context
            auto pActivity = Schedulers::ActivityFactory(*m_pScheduler, L"Delete Random");
            auto pArena = pActivity->GetArena();
            auto pPartition = m_pServer->FindPartitionForKey(descriptor.KeyHash);
            auto pContinuation = pArena->allocate<RepeatedDeleteContinuation>(*pActivity, pPartition, descriptor, &m_deleteIncomplete);

            // issue delete
            Audit::Assert(StatusCode::OK == pPartition->Delete(descriptor, pContinuation), "KeyValueStore->Delete");
            // in the completion   auto crc = Utilities::PsuedoCRC(pBuf->PBuffer(), length);

            --m_requestsRemaining;
            return 0 < m_requestsRemaining;
        }

        static bool DeleteAgainFunc(RepeatedAction& testGen)
        {
            return ((RepeatedDelete*)(&testGen))->DeleteOneMore();
        }

        void Run(int count) override
        {
            m_requestsRemaining = count;
            m_deleteIncomplete = count;
			RunAgainActor* deleteActor = m_pDeleteActivity->GetArena()->allocate<RunAgainActor>(*m_pDeleteActivity, *this, &DeleteAgainFunc);
            deleteActor->Post((intptr_t) this, 0, 0);

            uint32_t seconds = 0;
            while (0 < m_deleteIncomplete)
            {
                //Audit::Assert(seconds < count, "the test timed out for Read");
                EBTest::Trace("Delete count remaining %d", m_requestsRemaining);
                Sleep(1000);
                ++seconds;
            }
        }
    };

    std::unique_ptr<RepeatedAction> RepeatedDeleteFactory(ExabytesServer& server, Schedulers::Scheduler& scheduler, vector<pair<Datagram::PartialDescription, uint32_t>>& inventory)
    {
        return make_unique<RepeatedDelete>(server, scheduler, inventory);
    }

}
