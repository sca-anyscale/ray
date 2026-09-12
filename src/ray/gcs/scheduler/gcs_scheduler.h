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
//#include "ray/common/id.h"
//#include "ray/gcs/gcs_node_manager.h"
#include "ray/raylet/scheduling/cluster_lease_manager.h"
//#include "ray/util/counter_map.h"

namespace ray {
using raylet::ClusterLeaseManager;
namespace gcs {

using LeaseRequestCallback = std::function<void(const Status &status)>;

class GcsScheduler {
 public:
  /// Create a GcsScheduler
  ///
  /// \param cluster_lease_manager Used manage resource allocation
  /// \param gcs_lease_manager Used for node liveness checks
  /// \param io_context the IO context in which this component executes
  /// \param clock Clock utilities
#if 0
  GcsScheduler(ClusterLeaseManager &cluster_lease_manager,
                  GcsNodeManager &gcs_node_manager);
#else
  GcsScheduler(ClusterLeaseManager &cluster_lease_manager,
               instrumented_io_context &default_context,
               instrumented_io_context &scheduler_context);
#endif

  /// Queue lease and schedule. This happens when processing the worker lease request.
  ///
  /// \param lease: The incoming lease to be queued and scheduled.
  /// \param grant_or_reject: True if we we should either grant or reject the request
  ///                         but no spillback.
  /// \param is_selected_based_on_locality : should schedule on local node if possible.
  /// \param reply_callbacks: The reply callbacks of the lease request.
  void QueueAndScheduleLease(
      RayLease lease,
      bool grant_or_reject,
      bool is_selected_based_on_locality,
      std::vector<ray::raylet::internal::ReplyCallback> reply_callbacks);

  // Schedule and grant leases.
  void ScheduleAndGrantLeases();

  ClusterResourceManager &GetClusterResourceManager();

  bool IsLeaseQueued(const SchedulingClass &scheduling_class,
                     const LeaseID &lease_id) const;

  bool CancelLease(const LeaseID &lease_id,
                   rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
                       rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
                   const std::string &scheduling_failure_message = "");

  bool AddReplyCallback(const SchedulingClass &scheduling_class,
                        const LeaseID &lease_id,
                        rpc::SendReplyCallback send_reply_callback,
                        rpc::RequestWorkerLeaseReply *reply);

 private:
  ClusterLeaseManager &cluster_lease_manager_;
  instrumented_io_context &default_context_;
  instrumented_io_context &scheduler_context_;
  // GcsNodeManager &gcs_node_manager_;
};

}  // namespace gcs
}  // namespace ray
