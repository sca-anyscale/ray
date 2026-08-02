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
    rpc::RayletClientPool &raylet_client_pool,
    rpc::CoreWorkerClientPool &worker_client_pool,
    observability::RayEventRecorderInterface &ray_event_recorder,
    const std::string &session_name,
    // ray::observability::MetricInterface &actor_by_state_gauge,
    // ray::observability::MetricInterface &gcs_actor_by_state_gauge,
    pubsub::ObservabilityPublisher *observability_publisher,
    ClockInterface &clock)
    : cluster_lease_manager_(cluster_lease_manager),
      gcs_node_manager_(gcs_node_manager),
      io_context_(io_context),
      raylet_client_pool_(raylet_client_pool),
      worker_client_pool_(worker_client_pool),
      ray_event_recorder_(ray_event_recorder),
      session_name_(session_name),
      observability_publisher_(observability_publisher),
      clock_(clock) {}

void GcsLeaseManager::HandleGcsRequestWorkerLease(
    rpc::GcsRequestWorkerLeaseRequest request,
    rpc::GcsRequestWorkerLeaseReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto req = request.request();
  auto resp = reply->mutable_reply();
  auto lease_id = LeaseID::FromBinary(req.lease_spec().lease_id());

  RAY_CHECK(RayConfig::instance().centralized_actor_scheduling() == true);
  RAY_LOG(DEBUG) << "HANDLE1";
  // If the lease is already granted, this is a retry and forward the address of the
  // already leased worker to use
  if (known_leases_.contains(lease_id)) {
    const auto &lease_info = known_leases_[lease_id];
    auto worker_address = lease_info->Address();
    RAY_LOG(DEBUG) << "Lease " << lease_id
                   << " is already granted with worker: " << worker_address.worker_id();
    resp->set_worker_pid(lease_info->ProcessId());
    resp->mutable_worker_address()->set_ip_address(worker_address.ip_address());
    resp->mutable_worker_address()->set_port(worker_address.port());
    resp->mutable_worker_address()->set_worker_id(worker_address.worker_id());
    resp->mutable_worker_address()->set_node_id(worker_address.node_id());
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  // RayLease lease{req.lease_spec()};
  RayLease lease{std::move(*req.mutable_lease_spec())};

  RAY_CHECK(lease.GetLeaseSpecification().IsActorCreationTask() == false);

  auto send_reply_callback_wrapper =
      [this, lease, lease_id, request, reply, send_reply_callback](
          Status status,
          std::function<void()> success,
          std::function<void()> failure) mutable {
        RAY_LOG(DEBUG) << "STATUS " << status;
        auto rreq = request.mutable_request();
        auto rresp = reply->mutable_reply();
        const auto &retry_at_raylet_address = rresp->retry_at_raylet_address();
        RAY_LOG(DEBUG) << "GETLEASE " << rresp->worker_address().DebugString();
        RAY_LOG(DEBUG) << "GETLEASE " << retry_at_raylet_address.DebugString();
        auto node_id = NodeID::FromBinary(retry_at_raylet_address.node_id());
        auto node = gcs_node_manager_.GetAliveNode(node_id);
        RAY_CHECK(node.has_value());
        auto raylet_client =
            raylet_client_pool_.GetOrConnectByAddress(retry_at_raylet_address);

        rreq->mutable_lease_spec()->set_is_centrally_scheduled(true);
        rreq->set_grant_or_reject(true);
        raylet_client->RequestWorkerLease(
            std::move(*rreq),
            [this, lease, lease_id, rreq, reply, send_reply_callback](
                const Status &lease_status,
                const rpc::RequestWorkerLeaseReply &raylet_resp) {
              RAY_LOG(DEBUG) << "LSTATUS " << lease_status;
              const auto &final_address = raylet_resp.retry_at_raylet_address();
              RAY_LOG(DEBUG) << "GETLEASE2 "
                             << raylet_resp.worker_address().DebugString();
              RAY_LOG(DEBUG) << "GETLEASE2 " << final_address.DebugString();

              reply->mutable_reply()->CopyFrom(raylet_resp);

              if (lease_status.ok()) {
                // rpc::Address* address = raylet_resp.worker_address().New();
                // address->CopyFrom(raylet_resp.worker_address());
                auto lease_info = std::make_shared<LeaseInfo>(
                    lease, raylet_resp.worker_address(), raylet_resp.worker_pid());
                RAY_LOG(DEBUG) << "LEASEINFO " << lease_info->DebugString();
                known_leases_.emplace(lease_id, lease_info);
              }

              GCS_RPC_SEND_REPLY(send_reply_callback, reply, lease_status);
            });
      };

  if (cluster_lease_manager_.IsLeaseQueued(
          lease.GetLeaseSpecification().GetSchedulingClass(), lease_id)) {
    RAY_CHECK(cluster_lease_manager_.AddReplyCallback(
        lease.GetLeaseSpecification().GetSchedulingClass(),
        lease_id,
        std::move(send_reply_callback_wrapper),
        resp));
    return;
  }

  cluster_lease_manager_.QueueAndScheduleLease(
      std::move(lease),
      req.grant_or_reject(),
      req.is_selected_based_on_locality(),
      {raylet::internal::ReplyCallback(std::move(send_reply_callback_wrapper), resp)});
}

void GcsLeaseManager::HandleGcsReturnWorkerLease(
    rpc::GcsReturnWorkerLeaseRequest request,
    rpc::GcsReturnWorkerLeaseReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(RayConfig::instance().centralized_actor_scheduling() == true);
  // Read the resource spec submitted by the client.
  auto req = request.request();

  auto lease_id = LeaseID::FromBinary(req.lease_id());

  // Check if this message is a retry
  if (!known_leases_.contains(lease_id)) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  RAY_LOG(DEBUG).WithField(lease_id) << "LL0";
  auto lease_info = known_leases_[lease_id];
  RAY_LOG(DEBUG) << "LL1 " << lease_info->Lease().DebugString();
  // RAY_LOG(DEBUG) << "LLINFO " << lease_info->DebugString();
  auto worker_address = lease_info->Address();

  auto node_id = NodeID::FromBinary(worker_address.node_id());
  auto node = gcs_node_manager_.GetAliveNode(node_id);
  RAY_CHECK(node.has_value());

  auto raylet_client = raylet_client_pool_.GetOrConnectByAddress(worker_address);

  auto rreq = request.request();
  raylet_client->ReturnWorkerLease(worker_address.port(),
                                   lease_id,
                                   rreq.disconnect_worker(),
                                   rreq.disconnect_worker_error_detail(),
                                   rreq.worker_exiting());

  // remove lease from known_leases_
  known_leases_.erase(lease_id);

  // release the resources for the lease
  auto &cluster_resource_manager =
      cluster_lease_manager_.GetClusterResourceScheduler().GetClusterResourceManager();

  // XXX do we get placement resources for non-actors?
  RAY_LOG(DEBUG) << "LLINFO " << lease_info->DebugString();
  cluster_resource_manager.AddNodeAvailableResources(
      scheduling::NodeID(node_id.Binary()),
      lease_info->Lease().GetLeaseSpecification().GetRequiredPlacementResources());
  cluster_resource_manager.AddNodeAvailableResources(
      scheduling::NodeID(node_id.Binary()),
      lease_info->Lease().GetLeaseSpecification().GetRequiredResources());

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsLeaseManager::HandleGcsCancelWorkerLease(
    rpc::GcsCancelWorkerLeaseRequest request,
    rpc::GcsCancelWorkerLeaseReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto req = request.request();
  auto resp = reply->mutable_reply();
  const LeaseID lease_id = LeaseID::FromBinary(req.lease_id());

  RAY_CHECK(RayConfig::instance().centralized_actor_scheduling() == true);

  bool canceled = cluster_lease_manager_.CancelLease(lease_id);
  // The lease cancellation failed if we did not have the lease queued, since
  // this means that we may not have received the lease request yet. It is
  // successful if we did have the lease queued, since we have now replied to
  // the client that requested the lease.
  resp->set_success(canceled);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

}  // namespace gcs
}  // namespace ray
