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

#include "ray/gcs/lease/gcs_lease_manager.h"

namespace ray {
namespace gcs {

GcsLeaseManager::GcsLeaseManager(
    ClusterLeaseManager &cluster_lease_manager,
    GcsNodeManager &gcs_node_manager,
    instrumented_io_context &io_context,
    std::shared_ptr<PeriodicalRunnerInterface> periodical_runner,
    rpc::RayletClientPool &raylet_client_pool,
    ClockInterface &clock,
    NodeID local_node_id)
    : cluster_lease_manager_(cluster_lease_manager),
      gcs_node_manager_(gcs_node_manager),
      io_context_(io_context),
      periodical_runner_(periodical_runner),
      raylet_client_pool_(raylet_client_pool),
      clock_(clock),
      local_node_id_(std::move(local_node_id)) {
  periodical_runner_->RunFnPeriodically(
      [this]() { GCCancelledLeaseTombstones(); },
      RayConfig::instance().cancelled_lease_tombstone_ttl_ms(),
      "NodeManager.GCCancelledLeaseTombstones");
}

void GcsLeaseManager::HandleGcsRequestWorkerLease(
    rpc::GcsRequestWorkerLeaseRequest request,
    rpc::GcsRequestWorkerLeaseReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto req = request.mutable_request();
  req->mutable_lease_spec()->set_is_centrally_scheduled(true);
  bool grant_or_reject = req->grant_or_reject();
  req->set_grant_or_reject(true);

  auto resp = reply->mutable_reply();

  LeaseRequestCallback callback = [reply,
                                   send_reply_callback](const Status &lease_status) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, lease_status);
  };

  RequestWorkerLease(std::move(*req), resp, grant_or_reject, callback);
}

void GcsLeaseManager::RequestWorkerLease(const rpc::RequestWorkerLeaseRequest &request,
                                         rpc::RequestWorkerLeaseReply *reply,
                                         bool grant_or_reject,
                                         LeaseRequestCallback lease_request_callback) {
  RAY_CHECK(RayConfig::instance().centralized_actor_scheduling() == true);
  auto lease_id = LeaseID::FromBinary(request.lease_spec().lease_id());

  // If the lease is already granted, this is a retry and forward the address of the
  // already leased worker to use
  if (known_leases_.contains(lease_id)) {
    const auto &lease_info = known_leases_[lease_id];
    auto worker_address = lease_info->Address();
    RAY_LOG(DEBUG) << "Lease " << lease_id
                   << " is already granted with worker: " << worker_address.worker_id();
    reply->set_worker_pid(lease_info->ProcessId());
    reply->mutable_worker_address()->set_ip_address(worker_address.ip_address());
    reply->mutable_worker_address()->set_port(worker_address.port());
    reply->mutable_worker_address()->set_worker_id(worker_address.worker_id());
    reply->mutable_worker_address()->set_node_id(worker_address.node_id());
    ++counts_[CountType::RETRIED_REQUEST_WORKER_LEASE_REQUEST];
    lease_request_callback(Status::OK());
    return;
  }

  // Reject leases that were already cancelled (e.g. CancelWorkerLease arrived
  // before this RequestWorkerLease due to message reordering).
  if (cancelled_lease_tombstones_.contains(lease_id)) {
    reply->set_canceled(true);
    reply->set_failure_type(rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED);
    reply->set_scheduling_failure_message(
        "Cancelled leasing because the lease was already cancelled.");
    lease_request_callback(Status::OK());
    return;
  }

  RayLease lease{request.lease_spec()};

  RAY_CHECK(lease.GetLeaseSpecification().IsActorCreationTask() == false);

  auto send_reply_callback_wrapper =
      [this, lease, lease_id, request, reply, lease_request_callback](
          Status status,
          std::function<void()> success,
          std::function<void()> failure) mutable {
        RAY_LOG(DEBUG) << "STATUS " << status;
        const auto &retry_at_raylet_address = reply->retry_at_raylet_address();
        auto node_id = NodeID::FromBinary(retry_at_raylet_address.node_id());
        auto node = gcs_node_manager_.GetAliveNode(node_id);
        RAY_CHECK(node.has_value());
        auto raylet_client =
            raylet_client_pool_.GetOrConnectByAddress(retry_at_raylet_address);

        raylet_client->RequestWorkerLease(
            static_cast<rpc::RequestWorkerLeaseRequest &&>(
                const_cast<rpc::RequestWorkerLeaseRequest &>(request)),
            [this, node_id, lease, lease_id, reply, lease_request_callback](
                const Status &lease_status,
                const rpc::RequestWorkerLeaseReply &raylet_resp) {
              reply->CopyFrom(raylet_resp);

              if (lease_status.ok()) {
                auto lease_info = std::make_shared<LeaseInfo>(
                    lease, raylet_resp.worker_address(), raylet_resp.worker_pid());
                Insert(lease_id, lease_info, node_id);
              }

              ++counts_[CountType::REQUEST_WORKER_LEASE_REQUEST];
              lease_request_callback(lease_status);
            });
      };

  if (cluster_lease_manager_.IsLeaseQueued(
          lease.GetLeaseSpecification().GetSchedulingClass(), lease_id)) {
    RAY_CHECK(cluster_lease_manager_.AddReplyCallback(
        lease.GetLeaseSpecification().GetSchedulingClass(),
        lease_id,
        std::move(send_reply_callback_wrapper),
        reply));
    return;
  }

  cluster_lease_manager_.QueueAndScheduleLease(
      std::move(lease),
      grant_or_reject,
      request.is_selected_based_on_locality(),
      {raylet::internal::ReplyCallback(std::move(send_reply_callback_wrapper), reply)});
}

void GcsLeaseManager::HandleGcsReturnWorkerLease(
    rpc::GcsReturnWorkerLeaseRequest request,
    rpc::GcsReturnWorkerLeaseReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // Read the resource spec submitted by the client.
  auto req = request.request();

  ReturnWorkerLease(req);

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::RETURN_WORKER_LEASE_REQUEST];
}

void GcsLeaseManager::ReturnWorkerLease(const rpc::ReturnWorkerLeaseRequest &request) {
  RAY_CHECK(RayConfig::instance().centralized_actor_scheduling() == true);
  const LeaseID lease_id = LeaseID::FromBinary(request.lease_id());
  // Check if this message is a retry
  if (!known_leases_.contains(lease_id)) {
    return;
  }

  auto lease_info = known_leases_[lease_id];
  auto worker_address = lease_info->Address();

  auto node_id = NodeID::FromBinary(worker_address.node_id());
  auto node = gcs_node_manager_.GetAliveNode(node_id);
  RAY_CHECK(node.has_value());

  auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(worker_address);

  raylet_client->ReturnWorkerLease(worker_address.port(),
                                   lease_id,
                                   request.disconnect_worker(),
                                   request.disconnect_worker_error_detail(),
                                   request.worker_exiting());

  ReleaseLease(node_id, lease_id, lease_info->Lease(), true);
}

void GcsLeaseManager::ReleaseLease(const NodeID &node_id,
                                   const LeaseID &lease_id,
                                   const RayLease &lease,
                                   const bool erase) {
  if (!known_leases_.contains(lease_id)) {
    RAY_LOG(DEBUG).WithField(node_id).WithField(lease_id) << "UNKNOWN LEASE";
    ++counts_[CountType::UNKNOWN_LEASE_RELEASE];
    return;
  }

  // remove lease from known_leases_
  if (erase) {
    Erase(lease_id, node_id);
  }

  // release the resources for the lease
  auto &cluster_resource_manager =
      cluster_lease_manager_.GetClusterResourceScheduler().GetClusterResourceManager();

  // XXX do we get placement resources for non-actors?
  cluster_resource_manager.AddNodeAvailableResources(
      scheduling::NodeID(node_id.Binary()),
      lease.GetLeaseSpecification().GetRequiredPlacementResources());
}

void GcsLeaseManager::HandleGcsCancelWorkerLease(
    rpc::GcsCancelWorkerLeaseRequest request,
    rpc::GcsCancelWorkerLeaseReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto req = request.request();
  const LeaseID lease_id = LeaseID::FromBinary(req.lease_id());

  CancelWorkerLease(lease_id);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::CANCEL_WORKER_LEASE_REQUEST];
}

void GcsLeaseManager::CancelWorkerLease(const LeaseID &lease_id) {
  RAY_CHECK(RayConfig::instance().centralized_actor_scheduling() == true);
  RAY_CHECK(known_leases_.contains(lease_id) == false);

  // The tombstone makes the cancellation stick even if the lease request has not
  // reached us yet, so the caller never has to retry.
  AddCancelledLeaseTombstone(lease_id);

  cluster_lease_manager_.CancelLease(lease_id);
}

void GcsLeaseManager::OnNodeAdd(const NodeID &node_id) {
  if (node_leases_.contains(node_id)) {
    return;
  }

  node_leases_[node_id] = absl::flat_hash_map<LeaseID, std::shared_ptr<LeaseInfo>>();
}

void GcsLeaseManager::OnNodeDead(const NodeID &node_id) {
  const auto it = node_leases_.find(node_id);
  if (it == node_leases_.end()) {
    return;
  }
  std::vector<LeaseID> removed;
  for (const auto &[lease_id, lease_info] : it->second) {
    RAY_CHECK(NodeID::FromBinary(lease_info->Address().node_id()) == node_id);
    ReleaseLease(node_id, lease_id, lease_info->Lease(), false);
    removed.push_back(lease_id);
  }

  for (const auto &lease_id : removed) {
    Erase(lease_id, node_id);
  }
  node_leases_.erase(node_id);
  node_lease_versions_.erase(node_id);
}

void GcsLeaseManager::OnWorkerDead(const NodeID &node_id, const WorkerID &worker_id) {
  const auto it = node_leases_.find(node_id);
  if (it == node_leases_.end()) {
    return;
  }
  std::vector<LeaseID> removed;
  for (const auto &[lease_id, lease_info] : it->second) {
    if (WorkerID::FromBinary(lease_info->Address().worker_id()) == worker_id) {
      ReleaseLease(NodeID::FromBinary(lease_info->Address().node_id()),
                   lease_id,
                   lease_info->Lease(),
                   false);
      removed.push_back(lease_id);
    }
  }

  for (const auto &lease_id : removed) {
    Erase(lease_id, node_id);
  }
}

void GcsLeaseManager::ConsumeSyncMessage(
    std::shared_ptr<const syncer::RaySyncMessage> message) {
  io_context_.dispatch(
      [this, message]() {
        if (message->message_type() == rpc::syncer::MessageType::LEASE_ACK) {
          // LEASE_ACKs are not sent by the raylet, and we don't need our own
        } else if (message->message_type() == rpc::syncer::MessageType::LEASE_VIEW) {
          const NodeID node_id = NodeID::FromBinary(message->node_id());
          RAY_LOG(DEBUG).WithField(node_id)
              << "RECEIVE MESSAGE " << message->message_type();
          node_lease_versions_[node_id] = message->version();

          rpc::syncer::LeaseView lease_view_sync_message;
          lease_view_sync_message.ParseFromString(message->sync_message());

          RAY_CHECK(lease_view_sync_message.lease_status() ==
                        rpc::syncer::LeaseStatus::REMOVED ||
                    lease_view_sync_message.lease_status() ==
                        rpc::syncer::LeaseStatus::ACTIVE);

          if (lease_view_sync_message.lease_status() ==
              rpc::syncer::LeaseStatus::REMOVED) {
            RAY_LOG(DEBUG) << "REMOVE_LEASE";
            ReleaseLeases(node_id, lease_view_sync_message);
          } else {
            RAY_LOG(DEBUG) << "RESERVE_LEASE";
            ReserveLeases(node_id, lease_view_sync_message);
          }
          // we need a value that increases after a restart, rather than being reset
          syncer_version_ = clock_.SteadyNowMillis();
        } else {
          RAY_LOG(FATAL) << "Unsupported message type: " << message->message_type();
        }
      },
      "GcsLeaseManager::Update");
}

void GcsLeaseManager::ReleaseLeases(const NodeID &node_id,
                                    rpc::syncer::LeaseView &message) {
  for (const auto &lease : message.leases()) {
    RayLease ray_lease(lease.lease());
    RAY_CHECK(ray_lease.GetLeaseSpecification().IsActorCreationTask() == false);
    ReleaseLease(node_id, ray_lease.GetLeaseSpecification().LeaseId(), ray_lease, true);
    ++counts_[CountType::LEASES_RELEASED_BY_RAYLET];
  }
}

void GcsLeaseManager::ReserveLeases(const NodeID &node_id,
                                    rpc::syncer::LeaseView &message) {
  for (const auto &lease : message.leases()) {
    RayLease ray_lease(lease.lease());
    RAY_CHECK(ray_lease.GetLeaseSpecification().IsActorCreationTask() == false);
    ReserveLease(node_id, lease);
  }
}

void GcsLeaseManager::ReserveLease(const NodeID &node_id,
                                   const rpc::syncer::LeaseAndWorker &lease_message) {
  RayLease ray_lease(lease_message.lease());
  auto lease_id = ray_lease.GetLeaseSpecification().LeaseId();

  if (known_leases_.contains(lease_id)) {
    RAY_LOG(DEBUG).WithField(node_id).WithField(lease_id) << "DUP LEASE";
    ++counts_[CountType::DUP_LEASE_RESERVE];
    return;
  }

  auto lease_info = std::make_shared<LeaseInfo>(
      ray_lease, lease_message.address(), lease_message.pid());
  Insert(lease_id, lease_info, node_id);

  // acquire the resources for the lease
  auto &cluster_resource_manager =
      cluster_lease_manager_.GetClusterResourceScheduler().GetClusterResourceManager();

  auto resources = ResourceMapToResourceRequest(
      ray_lease.GetLeaseSpecification().GetRequiredPlacementResources().GetResourceMap(),
      true);
  cluster_resource_manager.SubtractNodeAvailableResources(
      scheduling::NodeID(node_id.Binary()), resources);

  ++counts_[CountType::LEASES_RESERVED_BY_RAYLET];
}

std::optional<syncer::RaySyncMessage> GcsLeaseManager::CreateSyncMessage(
    int64_t after_version, syncer::MessageType message_type) const {
  if (message_type != syncer::MessageType::LEASE_ACK) {
    return std::nullopt;
  }

  if (syncer_version_ <= after_version) {
    return std::nullopt;
  }

  syncer::RaySyncMessage msg;
  msg.set_version(syncer_version_);
  msg.set_node_id(local_node_id_.Binary());
  msg.set_message_type(syncer::MessageType::LEASE_ACK);

  rpc::syncer::LeaseAck lease_ack_msg;
  auto version_map = lease_ack_msg.mutable_node_versions();

  for (const auto &[node_id, version] : node_lease_versions_) {
    version_map->insert({node_id.Hex(), version});
  }

  std::string serialized_msg;
  RAY_CHECK(lease_ack_msg.SerializeToString(&serialized_msg));
  msg.set_sync_message(std::move(serialized_msg));

  return std::make_optional(std::move(msg));
}

std::string GcsLeaseManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsLeaseManager: "
         << "\n- RequestWorkerLease request count: "
         << counts_[CountType::REQUEST_WORKER_LEASE_REQUEST]
         << "\n- Retried RequestWorkerLease request count: "
         << counts_[CountType::RETRIED_REQUEST_WORKER_LEASE_REQUEST]
         << "\n- ReturnWorkerLease request count: "
         << counts_[CountType::RETURN_WORKER_LEASE_REQUEST]
         << "\n- CancelWorkerLease request count: "
         << counts_[CountType::CANCEL_WORKER_LEASE_REQUEST]
         << "\n- Unknown lease release count: "
         << counts_[CountType::UNKNOWN_LEASE_RELEASE]
         << "\n- Leases released by raylet count: "
         << counts_[CountType::LEASES_RELEASED_BY_RAYLET]
         << "\n- Duplicate leases reserved by raylet count: "
         << counts_[CountType::DUP_LEASE_RESERVE]
         << "\n- Leases reserved by raylet count: "
         << counts_[CountType::LEASES_RESERVED_BY_RAYLET]
         << "\n- Known leases: " << known_leases_.size();
  return stream.str();
}

void GcsLeaseManager::AddCancelledLeaseTombstone(const LeaseID &lease_id) {
  if (!cancelled_lease_tombstones_.insert(lease_id).second) {
    return;
  }
  cancelled_lease_tombstone_queue_.emplace_back(lease_id, clock_.SteadyNow());
  const auto max_tombstones = RayConfig::instance().max_cancelled_lease_tombstones();
  if (cancelled_lease_tombstones_.size() > max_tombstones) {
    const auto &oldest = cancelled_lease_tombstone_queue_.front();
    cancelled_lease_tombstones_.erase(oldest.first);
    cancelled_lease_tombstone_queue_.pop_front();
  }
}

void GcsLeaseManager::GCCancelledLeaseTombstones() {
  const auto ttl_ms =
      static_cast<int64_t>(RayConfig::instance().cancelled_lease_tombstone_ttl_ms());
  const auto now = clock_.SteadyNow();
  while (!cancelled_lease_tombstone_queue_.empty()) {
    const auto &oldest = cancelled_lease_tombstone_queue_.front();
    auto age_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - oldest.second)
            .count();
    if (age_ms <= ttl_ms) {
      break;
    }
    cancelled_lease_tombstones_.erase(oldest.first);
    cancelled_lease_tombstone_queue_.pop_front();
  }
}

}  // namespace gcs
}  // namespace ray
