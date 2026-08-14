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
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_node_manager.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/gcs/lease/lease_info.h"
#include "ray/ray_syncer/ray_syncer.h"
#include "ray/raylet/scheduling/cluster_lease_manager.h"
#include "ray/util/clock.h"
#include "ray/util/counter_map.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
using raylet::ClusterLeaseManager;
namespace gcs {

class GcsLeaseManager : public rpc::WorkerLeaseGcsServiceHandler,
                        public std::enable_shared_from_this<GcsLeaseManager>,
                        public syncer::ReporterInterface,
                        public syncer::ReceiverInterface {
 public:
  /// Create a GcsLeaseManager
  ///
  /// XXX comments
  GcsLeaseManager(ClusterLeaseManager &cluster_lease_manager,
                  GcsNodeManager &gcs_node_manager,
                  instrumented_io_context &io_context,
                  rpc::RayletClientPool &raylet_client_pool,
                  ClockInterface &clock,
                  NodeID local_node_id);

  void HandleGcsRequestWorkerLease(rpc::GcsRequestWorkerLeaseRequest request,
                                   rpc::GcsRequestWorkerLeaseReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  void HandleGcsReturnWorkerLease(rpc::GcsReturnWorkerLeaseRequest request,
                                  rpc::GcsReturnWorkerLeaseReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleGcsCancelWorkerLease(rpc::GcsCancelWorkerLeaseRequest request,
                                  rpc::GcsCancelWorkerLeaseReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  /// Handle a new node being added.
  ///
  /// \param node_id The specified node id.
  void OnNodeAdd(const NodeID &node_id);

  /// Handle a node death. This will remove the lease information for that node.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  /// Handle a worker failure. This will remove the lease information for that worker.
  ///
  /// \param node_id ID of the node where the dead worker was located.
  /// \param worker_id ID of the dead worker.
  void OnWorkerDead(const NodeID &node_id, const WorkerID &worker_id);

  void ConsumeSyncMessage(std::shared_ptr<const syncer::RaySyncMessage> message) override;
  std::optional<syncer::RaySyncMessage> CreateSyncMessage(
      int64_t after_version, syncer::MessageType message_type) const override;

  std::string DebugString() const;

 private:
  void ReserveLeases(const NodeID &node_id, rpc::syncer::LeaseView &message);
  void ReserveLease(const NodeID &node_id,
                    const rpc::syncer::LeaseAndWorker &lease_message);
  void ReleaseLeases(const NodeID &node_id, rpc::syncer::LeaseView &message);
  void ReleaseLease(const NodeID &node_id,
                    const LeaseID &lease_id,
                    const RayLease &lease,
                    const bool erase);

  ClusterLeaseManager &cluster_lease_manager_;
  GcsNodeManager &gcs_node_manager_;
  instrumented_io_context &io_context_;
  /// The cached raylet clients used to communicate with raylet.
  rpc::RayletClientPool &raylet_client_pool_;

  /// Map of leased workers to their worker address and lease specification.
  absl::flat_hash_map<LeaseID, std::shared_ptr<LeaseInfo>> known_leases_;
  /// Map of node IDs to their lease information
  absl::flat_hash_map<NodeID, absl::flat_hash_map<LeaseID, std::shared_ptr<LeaseInfo>>>
      node_leases_;

  ClockInterface &clock_;
  NodeID local_node_id_;

  absl::flat_hash_map<NodeID, int64_t> node_lease_versions_;
  int64_t syncer_version_ = clock_.SteadyNowMillis();

  // Debug info.
  enum CountType {
    REQUEST_WORKER_LEASE_REQUEST = 0,
    RETRIED_REQUEST_WORKER_LEASE_REQUEST = 1,
    RETURN_WORKER_LEASE_REQUEST = 2,
    CANCEL_WORKER_LEASE_REQUEST = 3,
    UNKNOWN_LEASE_RELEASE = 4,
    LEASES_RELEASED_BY_RAYLET = 5,
    DUP_LEASE_RESERVE = 6,
    LEASES_RESERVED_BY_RAYLET = 7,
    CountType_MAX = 8,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};
};

}  // namespace gcs
}  // namespace ray
