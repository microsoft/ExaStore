
#pragma once

#include "stdafx.h"

#include "ClientBroker.hpp"

namespace ExaBroker
{
    std::unique_ptr<ClientBroker> TestBrokerFactory(uint32_t serverIp, uint16_t serverPort);
}
