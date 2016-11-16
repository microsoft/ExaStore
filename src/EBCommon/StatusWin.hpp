
#pragma once

#include <winerror.h>

namespace Utilities
{
    //
    //
    //
    enum class StatusCode
    {
        OK               = S_OK                      , 
        AccessDenied     = E_ACCESSDENIED            ,
        DuplicatedId     = ERROR_DUPLICATE_TAG       ,
        Deleted          = ERROR_KEY_DELETED         ,
        FileExists       = ERROR_ALREADY_EXISTS      ,
        Incomplete       = ERROR_IO_INCOMPLETE       , 
        IncorrectSize    = ERROR_INCORRECT_SIZE      ,
        InvalidArgument  = E_INVALIDARG              ,
        InvalidFunction  = ERROR_INVALID_FUNCTION    ,
        InvalidHandle    = E_HANDLE                  , 
        InvalidParamter  = ERROR_INVALID_PARAMETER   ,
        InvalidState     = ERROR_INVALID_STATE       , 
        NotFound         = ERROR_NOT_FOUND           , 
        NotImplemented   = E_NOTIMPL                 , 
        OutOfBounds      = E_BOUNDS                  , 
        OutOfMemory      = E_OUTOFMEMORY             ,
        OutOfResources   = ERROR_NO_SYSTEM_RESOURCES ,
        Pending          = E_PENDING                 ,
        PendingIO        = ERROR_IO_PENDING          ,
        PermissionError  = ERROR_PRIVILEGE_NOT_HELD  , 
        StateChanged     = E_CHANGED_STATE           , 
        TimedOut         = ERROR_TIMEOUT             , 
        Unexpected       = E_UNEXPECTED              , 
        Abort            = E_ABORT                   ,
        UnspecifiedError = E_FAIL                    
    };
}