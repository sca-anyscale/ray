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

using LeaseRequestCallback = std::function<void(const Status &status)>;

class GcsLeaseManager : public rpc::WorkerLeaseGcsServiceHandler,
                        public std::enable_shared_from_this<GcsLeaseManager>,
                        public syncer::ReporterInterface,
                        public syncer::ReceiverInterface {
 public:
  /// Create a GcsLeaseManager
  ///
  /// \param cluster_lease_manager Used manage resource allocation
  /// \param gcs_lease_manager Used for node liveness checks
  /// \param io_context the IO context in which this component executes
  /// \param raylet_client_pool The client pool used to communicate with raylets
  /// \param clock Clock utilities
  /// \param local_node_id the local node ID of GCS itself
  GcsLeaseManager(ClusterLeaseManager &cluster_lease_manager,
                  GcsNodeManager &gcs_node_manager,
                  instrumented_io_context &io_context,
                  rpc::RayletClientPool &raylet_client_pool,
                  ClockInterface &clock,
                  NodeID local_node_id);

  /// Handle a worker lease request
  ///
  /// \param request a GCS lease request, which wraps the standard request
  /// \param reply a pointer to a GCS reply, which wraps the standard reply
  /// \param send_reply_callback the callback function which will process the reply
  void HandleGcsRequestWorkerLease(rpc::GcsRequestWorkerLeaseRequest request,
                                   rpc::GcsRequestWorkerLeaseReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  /// Handle returning a worker lease
  ///
  /// \param request a GCS return request, which wraps the standard request
  /// \param reply a pointer to a GCS reply, which wraps the standard reply
  /// \param send_reply_callback the callback function which will process the reply
  void HandleGcsReturnWorkerLease(rpc::GcsReturnWorkerLeaseRequest request,
                                  rpc::GcsReturnWorkerLeaseReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  /// Handle canceling a worker lease
  ///
  /// \param request a GCS cancellation request, which wraps the standard request
  /// \param reply a pointer to a GCS reply, which wraps the standard reply
  /// \param send_reply_callback the callback function which will process the reply
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

  /// Consume syncer messages (LEASE_VIEW)
  ///
  /// \param message the syncer message to process
  void ConsumeSyncMessage(std::shared_ptr<const syncer::RaySyncMessage> message) override;

  /// Create syncer messages (LEASE_ACK)
  /// returns std::nullopt if there is no new data to send
  ///
  /// \param after_version the last known version
  /// \param message_type the message type to create
  std::optional<syncer::RaySyncMessage> CreateSyncMessage(
      int64_t after_version, syncer::MessageType message_type) const override;

  /// used for periodic state logging
  std::string DebugString() const;

 private:
  /// Process a worker lease request
  ///
  /// \param request the lease being requested
  /// \param reply the gRPC reply message
  /// \param grant_or_reject the value of this field in the original request
  /// \param lease_request_callback the callback to run when sending a gRPC reply
  void RequestWorkerLease(const rpc::RequestWorkerLeaseRequest &request,
                          rpc::RequestWorkerLeaseReply *reply,
                          bool grant_or_reject,
                          LeaseRequestCallback lease_request_callback);
  /// Process returning a worker lease
  ///
  /// \param request the lease being returned
  void ReturnWorkerLease(const rpc::ReturnWorkerLeaseRequest &request);

  /// Process canceling a lease
  ///
  /// \param lease_id the ID of the lease being canceled
  /// \return the cancelation status
  bool CancelWorkerLease(const LeaseID &lease_id);

  /// reserve the active worker leases listed in the LeaseView message
  ///
  /// \param node_id the node ID for the leases
  /// \param message the lease view message
  void ReserveLeases(const NodeID &node_id, rpc::syncer::LeaseView &message);

  /// reserve the resources for the specified lease and track it
  ///
  /// \param node_id the node ID for the lease
  /// \param lease_message the lease and worker information
  void ReserveLease(const NodeID &node_id,
                    const rpc::syncer::LeaseAndWorker &lease_message);
  /// release the active worker leases listed in the LeaseView message
  ///
  /// \param node_id the node ID for the leases
  /// \param message the lease view message
  void ReleaseLeases(const NodeID &node_id, rpc::syncer::LeaseView &message);

  /// release the resources for the specified lease, optionally erasing it
  ///
  /// \param node_id the node ID for the lease
  /// \param lease_id the lease ID
  /// \param lease the lease information
  /// \param erase indicates whether the lease should be erased
  void ReleaseLease(const NodeID &node_id,
                    const LeaseID &lease_id,
                    const RayLease &lease,
                    const bool erase);

  /// insert a lease into the global and per-node maps
  ///
  /// \param lease_id the lease ID
  /// \param lease the lease information
  /// \param node_id the node ID for the lease
  void Insert(const LeaseID &lease_id,
              std::shared_ptr<LeaseInfo> lease_info,
              const NodeID &node_id) {
    known_leases_.emplace(lease_id, lease_info);

    auto &leases = node_leases_[node_id];
    leases.emplace(lease_id, lease_info);
  }

  /// remove a lease from the global and per-node maps
  ///
  /// \param lease_id the lease ID
  /// \param node_id the node ID for the lease
  void Erase(const LeaseID &lease_id, const NodeID &node_id) {
    known_leases_.erase(lease_id);
    const auto it = node_leases_.find(node_id);
    RAY_CHECK(it != node_leases_.end());
    it->second.erase(lease_id);
  }

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

  // used to set the syncer version of LEASE_ACK to monotonic time (needed for restarts)
  ClockInterface &clock_;

  // our local node ID -- used for creating syncer messages
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
