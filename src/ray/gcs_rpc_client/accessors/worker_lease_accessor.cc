// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/gcs_rpc_client/accessors/worker_lease_accessor.h"

#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/pubsub/gcs_subscriber.h"
#include "ray/util/container_util.h"

namespace ray {
namespace gcs {

WorkerLeaseAccessor::WorkerLeaseAccessor(GcsClientContext *context) : context_(context) {}

void WorkerLeaseAccessor::RequestWorkerLease(
    rpc::RequestWorkerLeaseRequest &&request,
    const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback) {
  auto grequest = rpc::GcsRequestWorkerLeaseRequest();
  grequest.mutable_request()->CopyFrom(request);

  context_->GetGcsRpcClient().GcsRequestWorkerLease(
      std::move(grequest),
      [callback](const Status &status, rpc::GcsRequestWorkerLeaseReply &&reply) {
        if (callback) {
          callback(status, std::move(*reply.mutable_reply()));
        }
      });
}

void WorkerLeaseAccessor::ReturnWorkerLease(
    int worker_port,
    const LeaseID &lease_id,
    bool disconnect_worker,
    const std::string &disconnect_worker_error_detail,
    bool worker_exiting) {
  rpc::GcsReturnWorkerLeaseRequest grequest;
  auto request = grequest.mutable_request();
  request->set_worker_port(worker_port);
  request->set_lease_id(lease_id.Binary());
  request->set_disconnect_worker(disconnect_worker);
  request->set_disconnect_worker_error_detail(disconnect_worker_error_detail);
  request->set_worker_exiting(worker_exiting);

  context_->GetGcsRpcClient().GcsReturnWorkerLease(
      std::move(grequest),
      [](const Status &status, rpc::GcsReturnWorkerLeaseReply &&reply /*unused*/) {
        RAY_LOG_IF_ERROR(INFO, status) << "Error returning worker: " << status;
      });
}

void WorkerLeaseAccessor::CancelWorkerLease(
    const LeaseID &lease_id,
    const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) {
  rpc::GcsCancelWorkerLeaseRequest grequest;
  auto request = grequest.mutable_request();
  request->set_lease_id(lease_id.Binary());

  context_->GetGcsRpcClient().GcsCancelWorkerLease(
      std::move(grequest),
      [callback](const Status &status, rpc::GcsCancelWorkerLeaseReply &&reply) {
        if (callback) {
          callback(status, std::move(*reply.mutable_reply()));
        }
      });
}

}  // namespace gcs
}  // namespace ray
