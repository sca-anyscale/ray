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

#pragma once

#include <functional>
#include <memory>

#include "absl/synchronization/mutex.h"
#include "ray/gcs_rpc_client/accessors/worker_lease_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"
#include "ray/rpc/rpc_callback_types.h"
#include "ray/util/sequencer.h"

namespace ray {
namespace gcs {

/**
  @class WorkerLeaseAccessor

  Implementation of WorkerLeaseAccessorInterface
 */
class WorkerLeaseAccessor : public WorkerLeaseAccessorInterface {
 public:
  WorkerLeaseAccessor() = default;
  explicit WorkerLeaseAccessor(GcsClientContext *context);
  virtual ~WorkerLeaseAccessor() = default;

  void RequestWorkerLease(
      rpc::RequestWorkerLeaseRequest &&request,
      const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback) override;

  void ReturnWorkerLease(int worker_port,
                         const LeaseID &lease_id,
                         bool disconnect_worker,
                         const std::string &disconnect_worker_error_detail,
                         bool worker_exiting) override;

  void CancelWorkerLease(
      const LeaseID &lease_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override;

 private:
  GcsClientContext *context_;
};

}  // namespace gcs
}  // namespace ray
