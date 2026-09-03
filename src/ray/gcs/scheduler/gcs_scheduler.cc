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

GcsScheduler::GcsScheduler(
    ClusterLeaseManager &cluster_lease_manager,
    GcsNodeManager &gcs_node_manager)
    : cluster_lease_manager_(cluster_lease_manager),
      gcs_node_manager_(gcs_node_manager) {}
}  // namespace gcs
}  // namespace ray
