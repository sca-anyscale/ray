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

#include <gtest/gtest_prod.h>

#include <list>
#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "ray/gcs/gcs_node_manager.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/gcs/lease/lease_info.h"
#include "ray/gcs/usage_stats_client.h"
#include "ray/observability/ray_event_recorder_interface.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/raylet/scheduling/cluster_lease_manager.h"
#include "ray/util/clock.h"
#include "ray/util/counter_map.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
using raylet::ClusterLeaseManager;
namespace gcs {

class GcsLeaseManager : public rpc::WorkerLeaseGcsServiceHandler,
                        public std::enable_shared_from_this<GcsLeaseManager> {
 public:
  /// Create a GcsLeaseManager
  ///
  /// XXX comments
  GcsLeaseManager(ClusterLeaseManager &cluster_lease_manager,
                  GcsNodeManager &gcs_node_manager,
                  instrumented_io_context &io_context,
                  rpc::RayletClientPool &raylet_client_pool,
                  rpc::CoreWorkerClientPool &worker_client_pool,
                  observability::RayEventRecorderInterface &ray_event_recorder,
                  const std::string &session_name,
                  pubsub::ObservabilityPublisher *observability_publisher,
                  ClockInterface &clock);

  void HandleGcsRequestWorkerLease(rpc::GcsRequestWorkerLeaseRequest request,
                                   rpc::GcsRequestWorkerLeaseReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  void HandleGcsReturnWorkerLease(rpc::GcsReturnWorkerLeaseRequest request,
                                  rpc::GcsReturnWorkerLeaseReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleGcsCancelWorkerLease(rpc::GcsCancelWorkerLeaseRequest request,
                                  rpc::GcsCancelWorkerLeaseReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

 private:
  ClusterLeaseManager &cluster_lease_manager_;
  GcsNodeManager &gcs_node_manager_;
  instrumented_io_context &io_context_;
  /// The cached raylet clients used to communicate with raylet.
  rpc::RayletClientPool &raylet_client_pool_;
  /// Core worker client pool shared by the GCS.
  rpc::CoreWorkerClientPool &worker_client_pool_;
  observability::RayEventRecorderInterface &ray_event_recorder_;
  std::string session_name_;
  pubsub::ObservabilityPublisher *observability_publisher_;
  ClockInterface &clock_;

  /// Map of leased workers to their worker address and lease specification.
  absl::flat_hash_map<LeaseID, std::shared_ptr<LeaseInfo>> known_leases_;

  // Debug info.
  enum CountType {
    REQUEST_WORKER_LEASE_REQUEST = 0,
    RETURN_WORKER_LEASE_REQUEST = 1,
    CANCEL_WORKER_LEASE_REQUEST = 2,
    GET_WORKER_FAILURE_CAUSE_REQUEST = 3,
    CountType_MAX = 4,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};
};

}  // namespace gcs
}  // namespace ray
