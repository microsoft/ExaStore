// NetBursts.cpp : Defines the entry point for the console application.
//

#pragma once

#include "stdafx.h"

#include <utility>
#include <algorithm>
#include <vector>
#include <stack>

#include "UtilitiesWin.hpp"

#include <winsock2.h>
#include <ws2tcpip.h>
#include <Ws2def.h>
// #include <Mswsock.h>
#include <iphlpapi.h>

using namespace std;
using namespace Utilities;

namespace   // private to this source file
{
    // Inspect the system and figure out all the IP Addresses we might be using.
    //
    size_t ListIpAddresses(vector<in_addr>& ipAddrs)
    {
        ipAddrs.clear();

        IP_ADAPTER_ADDRESSES adapterAddresses[256];
        ULONG adapterAddressesSize{ sizeof(adapterAddresses) };

        DWORD error = ::GetAdaptersAddresses(
            AF_INET,
            GAA_FLAG_SKIP_ANYCAST |
            GAA_FLAG_SKIP_MULTICAST |
            GAA_FLAG_SKIP_DNS_SERVER |
            GAA_FLAG_SKIP_FRIENDLY_NAME,
            NULL,
            adapterAddresses,
            &adapterAddressesSize);

        if (ERROR_SUCCESS != error)
        {
            return 0;
        }

        // Iterate through all of the adapters
        for (IP_ADAPTER_ADDRESSES* adapter = adapterAddresses; NULL != adapter; adapter = adapter->Next)
        {
            // Skip loopback and none-working adaptors 
            if (IF_TYPE_SOFTWARE_LOOPBACK == adapter->IfType || IfOperStatusUp != adapter->OperStatus)
            {
                continue;
            }
            if (IfOperStatusUp != adapter->OperStatus)
            {
                continue;
            }

            // Parse all IPv4 and IPv6 addresses
            for (IP_ADAPTER_UNICAST_ADDRESS* unicaster = adapter->FirstUnicastAddress; NULL != unicaster; unicaster = unicaster->Next)
            {
                auto family = unicaster->Address.lpSockaddr->sa_family;
                if (AF_INET == family)
                {
                    // IPv4
                    ipAddrs.push_back(reinterpret_cast<sockaddr_in*>(unicaster->Address.lpSockaddr)->sin_addr);
                }
                else if (AF_INET6 == family)
                {
                    // Skip all other types of addresses
                    continue;
                }
            }
        }
        return ipAddrs.size();
    }
}

namespace Utilities
{
    // locate "here" on the network
    //
    bool DiscoverHomeAddress(const SOCKET homeSocket, __out sockaddr_in& homeAddress, uint16_t port)
    {
        vector<in_addr> ipAddrs{};
        if (0 == ListIpAddresses(ipAddrs))
        {
            WSACleanup();
            Audit::OutOfLine::Fail("could not list adapter IP addresses");
        }

        sockaddr_in bindAddr;
        ZeroMemory(&bindAddr, sizeof(bindAddr));
        bindAddr.sin_family = AF_INET;
        bindAddr.sin_addr.S_un.S_addr = ipAddrs[0].S_un.S_addr;
        bindAddr.sin_port = htons(port);

        int iResult = ::bind(homeSocket, (SOCKADDR*) & bindAddr, sizeof(bindAddr));
        if (iResult != 0) {
            return false;
        }

        socklen_t addr_len = sizeof(homeAddress);
        iResult = getsockname(homeSocket, (struct sockaddr*)&homeAddress, &addr_len);
        if (0 != iResult) {
            return false;
        }
        return true;
    }

    //
    //  GetSystemErrorMessage
    //
    std::string
    GetSystemErrorMessage(const StatusCode sysErr)
    {
        DWORD dwFlags = FORMAT_MESSAGE_ALLOCATE_BUFFER |
                        FORMAT_MESSAGE_FROM_SYSTEM     |
                        FORMAT_MESSAGE_IGNORE_INSERTS  ;

        LPSTR lpBuf = NULL;
        FormatMessageA ( dwFlags                         ,    // dwFlags
                         NULL                            ,    // lpSource
                         (DWORD)sysErr                   ,    // dwMessageId
                         0                               ,    // dwLanguageId
                         reinterpret_cast<LPSTR>(&lpBuf) ,    // lpBuffer
                         0                               ,    // nSize
                         NULL                            );   // Arguments

        std::string ret = lpBuf;

        if (lpBuf)
            LocalFree(lpBuf);

        return ret;
    }
}