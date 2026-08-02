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

#include <optional>
#include <string>

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/**
  @interface WorkerLeaseAccessorInterface

  Interface for GCS's subset of lease management used for centralized task scheduling
 */
class WorkerLeaseAccessorInterface {
 public:
  virtual ~WorkerLeaseAccessorInterface() = default;

  virtual void RequestWorkerLease(
      rpc::RequestWorkerLeaseRequest &&request,
      const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback) = 0;

  virtual void ReturnWorkerLease(int worker_port,
                                 const LeaseID &lease_id,
                                 bool disconnect_worker,
                                 const std::string &disconnect_worker_error_detail,
                                 bool worker_exiting) = 0;

  virtual void CancelWorkerLease(
      const LeaseID &lease_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) = 0;
};

}  // namespace gcs
}  // namespace ray
