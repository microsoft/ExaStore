
#pragma once

#include "stdafx.h"

#include "TestHooks.hpp"

#include "UtilitiesWin.hpp"

namespace TestHooks
{
    LocalStoreTestHooks::LocalStoreTestHooks()
    {
        wchar_t* testdll = L"EBTest.dll";
        auto handle = GetModuleHandle(testdll);
        Audit::Assert(handle != NULL, "Test hook can only be used in EBTest.dll!");
    }

}