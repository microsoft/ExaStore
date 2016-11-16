#pragma once
#include "stdafx.h"

#include "stdint.h"

namespace Exabytes {

    //////////////////////////////////////////////////////////////
    // Jenkins Hash
    static const uint64_t sc_const = 0xdeadbeefdeadbeefULL;

    inline uint64_t Rot64(uint64_t x, int k)
    {
        return (x << k) | (x >> (64 - k));
    }

    inline void ShortMix(uint64_t &h0, uint64_t &h1, uint64_t &h2, uint64_t &h3)
    {
        h2 = Rot64(h2, 50);  h2 += h3;  h0 ^= h2;
        h3 = Rot64(h3, 52);  h3 += h0;  h1 ^= h3;
        h0 = Rot64(h0, 30);  h0 += h1;  h2 ^= h0;
        h1 = Rot64(h1, 41);  h1 += h2;  h3 ^= h1;
        h2 = Rot64(h2, 54);  h2 += h3;  h0 ^= h2;
        h3 = Rot64(h3, 48);  h3 += h0;  h1 ^= h3;
        h0 = Rot64(h0, 38);  h0 += h1;  h2 ^= h0;
        h1 = Rot64(h1, 37);  h1 += h2;  h3 ^= h1;
        h2 = Rot64(h2, 62);  h2 += h3;  h0 ^= h2;
        h3 = Rot64(h3, 34);  h3 += h0;  h1 ^= h3;
        h0 = Rot64(h0, 5);   h0 += h1;  h2 ^= h0;
        h1 = Rot64(h1, 36);  h1 += h2;  h3 ^= h1;
    }
    inline void ShortEnd(uint64_t &h0, uint64_t &h1, uint64_t &h2, uint64_t &h3)
    {
        h3 ^= h2;  h2 = Rot64(h2, 15);  h3 += h2;
        h0 ^= h3;  h3 = Rot64(h3, 52);  h0 += h3;
        h1 ^= h0;  h0 = Rot64(h0, 26);  h1 += h0;
        h2 ^= h1;  h1 = Rot64(h1, 51);  h2 += h1;
        h3 ^= h2;  h2 = Rot64(h2, 28);  h3 += h2;
        h0 ^= h3;  h3 = Rot64(h3, 9);   h0 += h3;
        h1 ^= h0;  h0 = Rot64(h0, 47);  h1 += h0;
        h2 ^= h1;  h1 = Rot64(h1, 54);  h2 += h1;
        h3 ^= h2;  h2 = Rot64(h2, 32);  h3 += h2;
        h0 ^= h3;  h3 = Rot64(h3, 25);  h0 += h3;
        h1 ^= h0;  h0 = Rot64(h0, 63);  h1 += h0;
    }

    inline uint32_t Spooky32(const uint32_t* key, size_t len, uint32_t seed)
    {
        union
        {
            const uint32_t *p32;
            const uint64_t *p64;
            size_t i;
        } u;

        u.p32 = key;

        size_t length = len << 2;
        size_t remainder = length & 31;
        uint64_t a = seed;
        uint64_t b = seed;
        uint64_t c = sc_const;
        uint64_t d = sc_const;

        if (length > 15)
        {
            const uint64_t *end = u.p64 + (length / 32) * 4;

            // handle all complete sets of 32 bytes
            for (; u.p64 < end; u.p64 += 4)
            {
                c += u.p64[0];
                d += u.p64[1];
                ShortMix(a, b, c, d);
                a += u.p64[2];
                b += u.p64[3];
            }

            //Handle the case of 16+ remaining bytes.
            if (remainder >= 16)
            {
                c += u.p64[0];
                d += u.p64[1];
                ShortMix(a, b, c, d);
                u.p64 += 2;
                remainder -= 16;
            }
        }

        // Handle the last 0..15 bytes, and its length
        d += ((uint64_t)length) << 56;
        switch (remainder)
        {
        case 12:
            d += u.p32[2];
            c += u.p64[0];
            break;
        case 8:
            c += u.p64[0];
            break;
        case 4:
            c += u.p32[0];
            break;
        case 0:
            c += sc_const;
            d += sc_const;
        }
        ShortEnd(a, b, c, d);

        return (uint32_t)a;
    }

    ////////////////////////////////////////////////////////////////
    // Murmur hash 3
    inline uint32_t rotl32(uint32_t x, int8_t r)
    {
        return (x << r) | (x >> (32 - r));
    }

    inline uint32_t fmix32(uint32_t h)
    {
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;

        return h;
    }

    inline uint32_t MurmurHash3_x86_32(const uint32_t* key, size_t len,
        uint32_t seed)
    {
        uint32_t h1 = seed;

        const uint32_t c1 = 0xcc9e2d51;
        const uint32_t c2 = 0x1b873593;

        for (int i = 0; i < len; i++)
        {
            uint32_t k1 = key[i];

            k1 *= c1;
            k1 = rotl32(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = rotl32(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        h1 = fmix32(h1);
        return h1;
    }


    //////////////////////////////////////////////////////////////////////
    // Bloom Filter
    class BloomBlock
    {
    public:
        static const size_t CAPACITY = 512;
        static const size_t BLOOMSIZE = CAPACITY / 4; // 2 bytes per key

        void Add(uint32_t bloomKey)
        {
            auto hash1 = bloomKey;
            auto hash2 = MurmurHash3_x86_32(&bloomKey, 1, 0x1EDC6F41);
            for (uint32_t h = 0; h < NUM_HASHES; h++) {
                auto hashValue = hash1 + h * hash2;
                SetBit(hashValue >> 4);
            }
        }

        bool Test(uint32_t bloomKey)
        {
            auto hash1 = bloomKey;
            auto hash2 = MurmurHash3_x86_32(&bloomKey, 1, 0x1EDC6F41);
            for (uint32_t h = 0; h < NUM_HASHES; h++) {
                auto hashValue = hash1 + h * hash2;
                if (!TestBit(hashValue >> 4)) {
                    return false;
                }
            }
            return true;
        }

        void Clear()
        {
            memset(m_bits, 0, sizeof(m_bits));
        }

    private:
        static const size_t NUM_HASHES = 11;

        uint64_t m_bits[BLOOMSIZE];     // we will use 6/16 Bloom, for 1/360 false positive rate
        static_assert((BLOOMSIZE & (BLOOMSIZE - 1)) == 0, "Must be power of 2 for each modular.");

        inline void SetBit(uint32_t index)
        {
            //index = (index ^ (index >> 3));
            m_bits[index >> 6 & (BLOOMSIZE - 1)] |= 1llu << (index & 63);
        }

        inline bool TestBit(uint32_t index)
        {
            //index = (index ^ (index >> 3));
            return (bool)((m_bits[(index >> 6) & (BLOOMSIZE - 1)] >> (index & 63)) & 1u);
        }

    };

}
