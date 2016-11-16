#pragma once
#include "stdafx.h"
#include "rpc.h"

#include <unordered_set>

#include "Utilities.hpp"
#include "Cuckoo.hpp"
#include "CppUnitTest.h"
#include "TestUtils.hpp"


using namespace std;
using namespace Microsoft::VisualStudio::CppUnitTestFramework;

using namespace Utilities;



namespace EBTest
{
    struct LongKey
    {
        uint64_t id[2];
        uint64_t parent;
        uint64_t span;
    };

}

namespace std
{
    // for std:: hash trait
    template<>
    struct hash<EBTest::LongKey>
    {
        inline uint64_t operator()(const EBTest::LongKey& value) const
        {
            return value.id[0] ^ value.id[1] ^ value.parent ^ value.span;
        }
    };

    template<>
    struct equal_to<EBTest::LongKey> {
        bool operator() (const EBTest::LongKey& x, const EBTest::LongKey& y) const {
            return x.id[0] == y.id[0] && x.id[1] == y.id[1]
                && x.parent == y.parent && x.span == y.span;
        }
    };

}

namespace EBTest
{
    static DummyOp<uint32_t> s_noop;

    TEST_CLASS(CuckooIndexTest)
    {
    private:
        LongKey m_testTable[4096];

        struct KeyOfHandle
        {
            const LongKey* const m_table;

            KeyOfHandle(const LongKey* table)
                : m_table(table)
            {}

            const LongKey& operator()(uint32_t h) const
            {
                return m_table[h];
            }
        };
        
        KeyOfHandle m_keyGetter;

        CuckooIndex<LongKey, uint32_t, ~0U, KeyOfHandle, true>
            m_cuckoo;
        uint32_t m_full = 0;

        void Fill()
        {
            for (uint32_t i = 0; i < 4090; i++)
            {
                CoCreateGuid((GUID*)&m_testTable[i]);
                CoCreateGuid((GUID*)&m_testTable[i].parent);

                auto tablestat = m_cuckoo.Insert(m_testTable[i], i);
                if (tablestat == Utilities::CuckooStatus::TableFull){
                    m_full = i;
                    EBTest::Trace("Table full at %d ", m_full);
                    break;
                }

                auto res = m_cuckoo.Find(m_testTable[i]);
                if (res != i){
                    EBTest::Trace("Error: can not find just inserted long key %d, status: %d\n", res, (uint32_t)tablestat);
                    Audit::OutOfLine::Fail("Error: can not find just inserted long key");
                }
            }

        }

    public:
        CuckooIndexTest()
            : m_keyGetter(m_testTable)
            , m_cuckoo(10, m_keyGetter, s_noop)
        {}

        TEST_METHOD(CuckooIndexRepeatFill)
        {
            for (unsigned i = 0; i < 1024; i++){
                Fill();
                m_cuckoo.Clear();
            }
        }

        TEST_METHOD(CuckooIndexFind)
        {
            Fill();
            for (uint32_t i = 0; i < m_full; i++)
            {
                auto res = m_cuckoo.Find(m_testTable[i]);
                if (res != i){
                    EBTest::Trace("Error: error in found %d, expected: %d\n", res, i);
                    Audit::OutOfLine::Fail("Error: can not find just inserted long key");
                }
            }
        }

        TEST_METHOD(CuckooIndexEraseUpdate){
            Fill();

            std::random_device m_seed;
            std::uniform_int_distribution<uint32_t> m_r;
            std::unordered_set<uint32_t> deleted;

            for (uint32_t i = 0; i < 2048; i++)
            {
                uint32_t index = m_r(m_seed) & 4095;
                auto k = deleted.insert(index);
                if (!(k.second)){
                    continue;
                }
                auto res = m_cuckoo.Erase(m_testTable[index]);
                if (index < m_full){
                    Audit::Assert(res, "Delete Failed!");
                }
                else {
                    Audit::Assert(!res, "Deleted phantom item!");
                }

                auto found = m_cuckoo.Find(m_testTable[index]);
                Audit::Assert(found == ~0U, "Deleted item found!");
            }

            for (uint32_t i = 0; i < m_full; i++){
                auto res = m_cuckoo.Find(m_testTable[i]);
                if (deleted.find(i) != deleted.end()){
                    Audit::Assert(res == ~0U, "Found deleted item");
                }
                else
                {
                    Audit::Assert(res == i, "NotFound after irrelavent delete!");
                }
            }

            for (uint32_t i = m_full - 1024; i <= m_full - 1024; i--)
            {
                m_testTable[i + 1] = m_testTable[i];
                auto tablestat = m_cuckoo.Update(m_testTable[i], i + 1);
                Audit::Assert(tablestat == Utilities::CuckooStatus::Ok,
                    "Update Failure");

                auto res = m_cuckoo.Find(m_testTable[i]);
                Audit::Assert(res == i + 1, "Error finding the updated.");
            }

            for (uint32_t i = 1; i < m_full - 1025; i++)
            {
                auto res = m_cuckoo.Find(m_testTable[i]);
                Audit::Assert(res == i, "Error: irrelavent update screwed up index");
            }
        }

    };

    TEST_CLASS(CuckooTableTest)
    {
        
    private:
        static const uint32_t MAX = 2860;
        LongKey m_testTable[MAX];
        CuckooTable<LongKey, uint32_t> m_table;
    public:
        CuckooTableTest()
            : m_table(MAX)
        {}

        void Fill()
        {
            for (uint32_t i = 0; i < MAX; i++)
            {
                CoCreateGuid((GUID*)&m_testTable[i]);
                CoCreateGuid((GUID*)&m_testTable[i].parent);

                auto tablestat = m_table.Insert(std::make_pair(m_testTable[i], i));

                Audit::Assert(tablestat.second == Utilities::CuckooStatus::Ok,
                    "Insertion failure?");
                // table full? research says it should have 93% load factor, we are
                // only using 87%? very bad luck?

                auto res = m_table.Find(m_testTable[i]);
                if (res->second != i){
                    EBTest::Trace("Error: can not find just inserted long key %d, status: %d\n", res->second, (uint32_t)tablestat.second);
                    Audit::OutOfLine::Fail("Error: can not find just inserted long key");
                }
                Audit::Assert(tablestat.first == res, "insertion and found iterator should be the same!");

                // duplication insertion
                tablestat = m_table.Insert(std::make_pair(m_testTable[i], i+56));
                Audit::Assert(tablestat.first == res && tablestat.second == CuckooStatus::KeyDuplicated,
                    "Duplicated key expected!");
            }

        }

        TEST_METHOD(CuckooTableFind){
            Fill();
            for (uint32_t i = 1; i < MAX; i++)
            {
                auto res = m_table.Find(m_testTable[i]);
                Audit::Assert(res->second == i, "Error in locating inserted item");
            }
        }

        TEST_METHOD(CuckooTableDelete){
            Fill();

            std::random_device m_seed;
            std::uniform_int_distribution<uint32_t> m_r;
            std::unordered_set<uint32_t> deleted;

            for (uint32_t i = 0; i < MAX/2; i++)
            {
                uint32_t index = m_r(m_seed) & 4095;
                if (index >= MAX)
                    continue;

                auto k = deleted.insert(index);
                if (!(k.second)){
                    continue;
                }
                auto res = m_table.Erase(m_testTable[index]);
                Audit::Assert(res, "Delete failed!");

                auto found = m_table.Find(m_testTable[index]);
                Audit::Assert(found == m_table.end(), "Deleted item found!");
            }


            for (uint32_t i = 1; i < MAX; i++){
                auto res = m_table.Find(m_testTable[i]);
                if (deleted.find(i) != deleted.end()){
                    Audit::Assert(res == m_table.end(), "Found deleted item.");
                }
                else
                {
                    Audit::Assert(res->second == i, "Error: irrelavent delete screwed up table");
                }
            }
        }


    };
}
