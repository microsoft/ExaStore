// NetBursts.cpp : Defines the entry point for the console application.
//

#pragma once

#include "stdafx.h"

#include "ServiceBroker.hpp"

namespace ServiceBrokers
{
    // instantiate a broker associated with registered buffers and IO Completion Port.
    //
    extern std::unique_ptr<ServiceBroker> ServiceBrokerRIOFactory(
        _In_ const std::wstring& name,      // the name of the service
        uint16_t port = 0
        );
}