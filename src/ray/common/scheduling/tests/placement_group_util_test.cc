// Copyright 2025 The Ray Authors.
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

#include "ray/common/scheduling/placement_group_util.h"

#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/bundle_spec.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

TEST(PlacementGroupUtilTest, BasicFormatter) {
  rpc::Bundle bundle;

  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  auto *bundle_id_msg = bundle.mutable_bundle_id();
  bundle_id_msg->set_placement_group_id(group_id.Binary());
  bundle_id_msg->set_bundle_index(1);
  bundle.mutable_unit_resources()->insert({"CPU", 1.0});
  BundleSpecification spec(bundle);
  for (const auto &resource_entry : spec.GetFormattedResources()) {
    ASSERT_TRUE(resource_entry.first.find(kCompositeGroupPrefix) == std::string::npos);
  }
}

TEST(PlacementGroupUtilTest, ComboFormatter) {
  rpc::Bundle bundle;

  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  auto *bundle_id_msg = bundle.mutable_bundle_id();
  bundle_id_msg->set_placement_group_id(group_id.Binary());
  bundle_id_msg->set_bundle_index(1);
  bundle.mutable_unit_resources()->insert({"CPU", 1.0});
  bundle.mutable_unit_resources()->insert({"GPU", 1.0});
  BundleSpecification spec(bundle);
  for (const auto &resource_entry : spec.GetFormattedResources()) {
    ASSERT_EQ(0, resource_entry.first.find(kCompositeGroupPrefix));
  }
}

TEST(PlacementGroupUtilTest, BasicParser) {
  rpc::Bundle bundle;

  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  auto *bundle_id_msg = bundle.mutable_bundle_id();
  bundle_id_msg->set_placement_group_id(group_id.Binary());
  bundle_id_msg->set_bundle_index(1);
  bundle.mutable_unit_resources()->insert({"CPU", 1.0});
  BundleSpecification spec(bundle);

  for (const auto &resource_entry : spec.GetFormattedResources()) {
    ASSERT_TRUE(resource_entry.first.find(kCompositeGroupPrefix) == std::string::npos);
    std::optional<PgFormattedResourceData> resource;
    if (std::count(resource_entry.first.begin(), resource_entry.first.end(), '_') > 2) {
      resource = ParsePgFormattedResource(resource_entry.first, false, true);
    } else {
      resource = ParsePgFormattedResource(resource_entry.first, true, false);
    }
    ASSERT_TRUE(resource.has_value());
    ASSERT_EQ(resource_entry.first, resource.value().requested_resource);
    if (resource_entry.first.find(kBundle_ResourceLabel) == std::string::npos) {
      ASSERT_EQ("CPU", resource.value().original_resource);
    } else {
      ASSERT_EQ(kBundle_ResourceLabel, resource.value().original_resource);
    }
    ASSERT_EQ(group_id.Hex(), resource.value().group_id);
    if (std::count(resource_entry.first.begin(), resource_entry.first.end(), '_') > 2) {
      ASSERT_EQ(1, resource.value().bundle_index);
    }
  }
}

TEST(PlacementGroupUtilTest, ComboParser) {
  rpc::Bundle bundle;

  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  auto *bundle_id_msg = bundle.mutable_bundle_id();
  bundle_id_msg->set_placement_group_id(group_id.Binary());
  bundle_id_msg->set_bundle_index(1);
  bundle.mutable_unit_resources()->insert({"CPU", 1.0});
  bundle.mutable_unit_resources()->insert({"GPU", 1.0});
  BundleSpecification spec(bundle);

  for (const auto &resource_entry : spec.GetFormattedResources()) {
    ASSERT_EQ(0, resource_entry.first.find(kCompositeGroupPrefix));
    std::optional<PgFormattedResourceData> resource;
    if (std::count(resource_entry.first.begin(), resource_entry.first.end(), '_') > 3) {
      resource = ParsePgFormattedResource(resource_entry.first, false, true);
    } else {
      resource = ParsePgFormattedResource(resource_entry.first, true, false);
    }
    ASSERT_TRUE(resource.has_value());
    if (resource_entry.first.find(kBundle_ResourceLabel) == std::string::npos) {
      ASSERT_TRUE("CPU" == resource.value().original_resource ||
                  "GPU" == resource.value().original_resource);
    } else {
      ASSERT_EQ(kComboBundle_ResourceLabel, resource.value().original_resource);
    }
    ASSERT_EQ(group_id.Hex(), resource.value().group_id);
    if (std::count(resource_entry.first.begin(), resource_entry.first.end(), '_') > 3) {
      ASSERT_EQ(1, resource.value().bundle_index);
    }
  }
}

}  // namespace ray
