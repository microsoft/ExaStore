// NetBursts.cpp : Defines the entry point for the console application.
//

#pragma once

#include "stdafx.h"
#include "Scheduler.hpp"
#include "UtilitiesWin.hpp"

#include <memory>
#include <stack>
#include <vector>

using namespace std;

namespace Schedulers
{
    static const size_t ALIGNMENT = 16;

	ActionArena::ActionArena(ArenaSet& set, void* bytes, uint32_t sizeLimit)
		: m_sizeLimit(sizeLimit)
		, m_disposableOffset(sizeLimit)
		, m_pSet(&set)
		, m_arenaBytes(bytes)
		, m_offset(0)
	{}

    void ActionArena::Retire()
    {
		Audit::Assert(0 != m_offset, "retiring the same action twice?");
        m_offset = 0;

		// Call all registered Dispose fuctions.
		auto pDisposable = (Utilities::Disposable**)(m_disposableOffset + (char*)m_arenaBytes);
		auto pDisposableEnd = (Utilities::Disposable**)(m_sizeLimit + (char*)m_arenaBytes);

		while (pDisposable < pDisposableEnd)
		{
			Audit::Assert(nullptr != *pDisposable, "Disposable object is null!");
			(*pDisposable)->Dispose();
			pDisposable++;
		}

		m_disposableOffset = m_sizeLimit;

        // This code is on the critical path, we use zero memory
        // to ensure safety, however it may have performance impact
        // TODO!! performance experiment to decide whether to keep this.
        // ZeroMemory(m_arenaBytes, m_sizeLimit);
        m_pSet->Push(this);
    }

	ActionArena* ContinuationBase::GetArena() const
	{
		return m_activity.GetScheduler()->GetArena((void*)this);
	}


    class ThreadSafeArenaSet : public ArenaSet
    {
		vector<ActionArena*> m_allArenas;
		const size_t m_unitSize;

        std::unique_ptr<Utilities::LargePageBuffer> m_pBuffer;
        BoundedBuf<ActionArena*> m_arenas;

    public:
        ThreadSafeArenaSet(size_t unitSize, uint32_t count)
            : m_unitSize(unitSize), m_arenas(count)
        {
            Audit::ArgRule(((unitSize - 1) ^ (0 - unitSize)) == ~0, "unitSize must be a power of 2");
            Audit::ArgRule(unitSize <= (1ull << 26), "unitSize must be less than or equal to 64MB");
            Audit::ArgRule(0 < count, "non zero count expected");

            Audit::Assert((unitSize * count) < (1ull << 30), "limit total of arenas to 1GB");

            size_t actualSize;
            unsigned actualCount;
            m_pBuffer = std::make_unique<Utilities::LargePageBuffer>(unitSize, count, actualSize, actualCount);
            for (unsigned i = 0; i < actualCount; ++i)
            {
				auto pArena = new ActionArena{ *this, m_pBuffer->PData(i * unitSize), (uint32_t) unitSize };
				m_allArenas.push_back(pArena);
                m_arenas.Add(pArena);
            }
        }

        ActionArena* Pop()
        {
			ActionArena* pArena = m_arenas.Get();
            Tracer::LogCounterValue(Tracer::EBCounter::AvailableArenas, L"AvailableThreadSafeArenas", m_arenas.GetSize());
			Audit::Assert(pArena->IsEmpty(), "popped an arena which is still in use");
            return pArena;
        }

        void Push(ActionArena* pArena)
        {
            Audit::Assert(pArena->IsEmpty(), "pushing an arena which is still in use");
            m_arenas.Add(pArena);                        
        }

		ActionArena* GetActionArena(void* pData)
		{
			size_t arenaIndex = (size_t) m_pBuffer->OffsetOf(pData) / m_unitSize;
			Audit::Assert(arenaIndex < m_allArenas.size(), "the argument did not point inside the ArenaSet");
			return m_allArenas[arenaIndex];
		}

        ~ThreadSafeArenaSet()
		{
			for (auto pArena : m_allArenas)
				delete pArena;
		}
    };

    unique_ptr<ArenaSet> ArenaSet::ArenaSetFactory(size_t unitSize, uint32_t count)
    {
        return make_unique<ThreadSafeArenaSet>(unitSize, count);
    }

}