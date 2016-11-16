// Rpc context for client and server
//

#pragma once

#include "stdafx.h"

#include "Utilities.hpp"

#include "Scheduler.hpp"
#include "ServiceBroker.hpp"
#include "StatusWin.hpp"

namespace Utilities
{
    // Interface for initiating a request/response duo from a client point
    // of view. Implementations should take care of idempotent message retry.
    // Implementation should also reacting on response messages by binding
    // verbs in ServiceBroker. 
    //
    // Note that now the the only way we distinguish an incoming request
    // from a response message is by ServiceVerb.
    // 
    class RpcClient {
    public:
        // Send a RPC request to a remote server,
        //
        // pCompletion triggered when the response is received or timed out.
        // pMsgBuf contains the outgoing message.
        //
        virtual StatusCode SendReq(
            // Triggered when finished
            // if the reponse's ServiceVerb is Status, status code is posted to
            // pCompletion's continuationHandle. Otherwise, a BufferEnvelope
            // containing the response message (started with Request structure)
            // is posted to pCompletion, assuming pCompletion is an instance
            // of Continuation<BufferEnvelope>
            //
            Schedulers::ContinuationBase* pCompletion,

            // Outgoing message. Buffer is released when call is finished.
            // The buff should begin with a Request structure, where the 
            // caller specifies remote address and action using RemoteIPv4
            // RemotePort and Verb fields
            //
            DisposableBuffer* pMsgBuf
            ) = 0;

        virtual ~RpcClient() {};
    };


    // Construct a RpcClient with exponential backup retry, and Cuckoo map
    // as context match.
    //
    extern std::unique_ptr<RpcClient> ExpBackupRetryRpcClientFactory(
        // Network module
        _In_ ServiceBrokers::ServiceBroker& broker,

        // An array of ServiceVerbs for which this RpcClient object
        // would consider to be responses of Rpc calls.
        //
        _In_ const Datagram::ServiceVerb responseVerbs[],
        _In_ const size_t numVerbs
        );

}