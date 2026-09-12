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

#include "ray/gcs/scheduler/gcs_scheduler.h"

namespace ray {
namespace gcs {

#if 0
GcsScheduler::GcsScheduler(
    ClusterLeaseManager &cluster_lease_manager,
    GcsNodeManager &gcs_node_manager)
    : cluster_lease_manager_(cluster_lease_manager),
      gcs_node_manager_(gcs_node_manager) {}
#else
GcsScheduler::GcsScheduler(ClusterLeaseManager &cluster_lease_manager,
                           instrumented_io_context &default_context,
                           instrumented_io_context &scheduler_context)
    : cluster_lease_manager_(cluster_lease_manager),
      default_context_(default_context),
      scheduler_context_(scheduler_context) {}
#endif

ClusterResourceManager &GcsScheduler::GetClusterResourceManager() {
  return cluster_lease_manager_.GetClusterResourceScheduler().GetClusterResourceManager();
}

void GcsScheduler::QueueAndScheduleLease(
    RayLease lease,
    bool grant_or_reject,
    bool is_selected_based_on_locality,
    std::vector<ray::raylet::internal::ReplyCallback> reply_callbacks) {
  scheduler_context_.dispatch(
      [this, lease, grant_or_reject, is_selected_based_on_locality, reply_callbacks]() {
        cluster_lease_manager_.QueueAndScheduleLease(
            lease, grant_or_reject, is_selected_based_on_locality, reply_callbacks);
      },
      "GcsScheduler::ScheduleAndGrantLeases");
}

void GcsScheduler::ScheduleAndGrantLeases() {
  scheduler_context_.dispatch(
      [this]() { cluster_lease_manager_.ScheduleAndGrantLeases(); },
      "GcsScheduler::ScheduleAndGrantLeases");
}

bool GcsScheduler::IsLeaseQueued(const SchedulingClass &scheduling_class,
                                 const LeaseID &lease_id) const {
  return cluster_lease_manager_.IsLeaseQueued(scheduling_class, lease_id);
}

bool GcsScheduler::CancelLease(
    const LeaseID &lease_id,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  return cluster_lease_manager_.CancelLease(
      lease_id, failure_type, scheduling_failure_message);
}

bool GcsScheduler::AddReplyCallback(const SchedulingClass &scheduling_class,
                                    const LeaseID &lease_id,
                                    rpc::SendReplyCallback send_reply_callback,
                                    rpc::RequestWorkerLeaseReply *reply) {
  auto reply_callback_wrapper =
      [this, send_reply_callback](
          Status status, std::function<void()> success, std::function<void()> failure) {
        default_context_.post(
            [send_reply_callback, status, success, failure]() {
              send_reply_callback(status, success, failure);
            },
            "GcsScheduler::ReplyCallback");
      };

  return cluster_lease_manager_.AddReplyCallback(
      scheduling_class, lease_id, reply_callback_wrapper, reply);
}

}  // namespace gcs
}  // namespace ray
