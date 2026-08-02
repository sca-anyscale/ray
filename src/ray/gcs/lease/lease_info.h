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

#include <sys/types.h>
#include <sys/wait.h>

#include "ray/common/lease/lease.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

class LeaseInfo {
 public:
  LeaseInfo(const ray::RayLease lease, const rpc::Address address, const pid_t process_id)
      : lease_(std::move(lease)), address_(std::move(address)), process_id_(process_id){};

  const rpc::Address &Address() const { return address_; }

  const ray::RayLease &Lease() const { return lease_; }

  const pid_t ProcessId() const { return process_id_; }

  std::string DebugString() const {
    return absl::StrFormat("address={%s}, PID={%u}, lease={%s}",
                           address_.DebugString(),
                           process_id_,
                           lease_.DebugString());
  }

 private:
  const ray::RayLease lease_;
  const rpc::Address address_;
  const pid_t process_id_;
};

}  // namespace gcs
}  // namespace ray
