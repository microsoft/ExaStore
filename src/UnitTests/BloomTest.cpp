#pragma once
#include "stdafx.h"

#include <iostream>
#include <vector>
#include <unordered_set>
#include <ctime>

#include "Bloom.hpp"
#include "Catalog.hpp"
#include "UtilitiesWin.hpp"
#include "TestHooks.hpp"
#include "TestUtils.hpp"

#include "CppUnitTest.h"

namespace EBTest
{
	using namespace std;
	using namespace Datagram;
	using namespace Schedulers;
	using namespace Exabytes;
	using namespace Microsoft::VisualStudio::CppUnitTestFramework;
	using namespace Utilities;

	TEST_CLASS(BloomTest)
	{
		BloomBlock m_bFilter;
		std::mt19937_64  m_randomSeed;
        std::random_device m_dv;
		std::uniform_int_distribution<uint64_t> m_random;
	public:
		TEST_METHOD(BloomHitRate)
		{
			auto seed = m_random(m_dv);
            m_randomSeed.seed(seed);

			std::unordered_set<Key128> keyset;
			std::unordered_set<uint32_t> foldedKeys;

			for (int i = 0; i < BloomBlock::CAPACITY; i++) {
				Key128 key(m_random(m_randomSeed), m_random(m_randomSeed));
				auto halfKey = Reduction::HalveKey(key);
				auto bloomKey = Reduction::QuarterKey(key.ValueLow(), seed);
				m_bFilter.Add(bloomKey);
				auto res = keyset.insert(key);
				if (!res.second) {
					EBTest::Trace("key set colision");
				}
				auto fres = foldedKeys.insert(bloomKey);
				if (!fres.second) {
					EBTest::Trace("folded key collision when setting up.");
				}
			}

			for (auto k : keyset) {
				auto halfKey = Reduction::HalveKey(k);
				auto bloomKey = Reduction::QuarterKey(k.ValueLow(), seed);
				if (!m_bFilter.Test(bloomKey)) {
					Audit::OutOfLine::Fail(Utilities::StatusCode::Unexpected,
						"Inserted key not detected.");
				}
			}

			size_t hit = 0;
			clock_t begin = clock();
			for (int j = 0; j < 1000000; j++) {
				Key128 key(m_random(m_randomSeed), m_random(m_randomSeed));
				if (keyset.find(key) != keyset.end()) {
					EBTest::Trace("key generation collide with key set");
					j--;
					continue;
				}

				auto halfKey = Reduction::HalveKey(key);
				auto bloomKey = Reduction::QuarterKey(key.ValueLow(), seed);
				if (foldedKeys.find(bloomKey) != foldedKeys.end()) {
					EBTest::Trace("Folded key collision!");
				}


				if (m_bFilter.Test(bloomKey)) {
					hit++;
				}
				if (j % (1000000 / 8) == 0) {
					EBTest::Trace("Bloom filter hit: %d", hit);
    				Audit::Assert(hit < 2000, "Bloom false hit rate too big! > 0.2%");
				}
			}
			clock_t end = clock();
			double elapsed = double(end - begin) / CLOCKS_PER_SEC;

			EBTest::Trace("Bloom filter hit: %d", hit);
			EBTest::Trace("Elapsed time in seconds: %f", elapsed);
			Audit::Assert(hit < 2000, "Bloom false hit rate too big! > 0.2%");
			
		}
	};
}