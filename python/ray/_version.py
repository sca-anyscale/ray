# Replaced with the current commit when building the wheels.
commit = "c1f006b83d70d6057fb73b6e7d639e225e5f6819"
version = "3.0.0.dev0"

if __name__ == "__main__":
    print("%s %s" % (version, commit))
"""
diff --git a/python/ray/_raylet.pyx b/python/ray/_raylet.pyx
index 95a3904410..20fa3b1a0b 100644
--- a/python/ray/_raylet.pyx
+++ b/python/ray/_raylet.pyx
@@ -4216,6 +4216,22 @@ cdef class CoreWorker:
         # call to AsyncWaitPlacementGroupReady.
         return ObjectRef(c_object_id.Binary(), skip_adding_local_ref=True)

+    def lock_scheduler(self, timeout=2):
+        cdef CRayStatus status
+        status = CCoreWorkerProcess.GetCoreWorker() \
+            .LockScheduler()
+        while status.IsAlreadyExists():
+            time.sleep(timeout)   # exponential?
+            status = CCoreWorkerProcess.GetCoreWorker() \
+                .LockScheduler()
+        return status.ok()
+
+    def unlock_scheduler(self):
+        cdef CRayStatus status
+        status = CCoreWorkerProcess.GetCoreWorker() \
+            .UnlockScheduler()
+        return status.ok()
+
     def submit_actor_task(self,
                           Language language,
                           ActorID actor_id,
diff --git a/python/ray/includes/common.pxd b/python/ray/includes/common.pxd
index b07f78a59b..661f38d7f6 100644
--- a/python/ray/includes/common.pxd
+++ b/python/ray/includes/common.pxd
@@ -18,6 +18,7 @@ from ray.includes.unique_ids cimport (
     CTaskID,
     CPlacementGroupID,
     CNodeID,
+    CLockID,
 )
 from ray.includes.function_descriptor cimport (
     CFunctionDescriptor,
diff --git a/python/ray/includes/libcoreworker.pxd b/python/ray/includes/libcoreworker.pxd
index ca47ff1629..a5ca6e3226 100644
--- a/python/ray/includes/libcoreworker.pxd
+++ b/python/ray/includes/libcoreworker.pxd
@@ -22,6 +22,7 @@ from ray.includes.unique_ids cimport (
     CPlacementGroupID,
     CWorkerID,
     ObjectIDIndexType,
+    CLockID,
 )

 from ray.includes.common cimport (
@@ -183,6 +184,8 @@ cdef extern from "ray/core_worker/core_worker.h" nogil:
                               c_bool recursive)
         c_bool IsTaskCanceled(const CTaskID &task_id) const
         c_bool ShouldInterruptTaskForCancellation() const
+        CRayStatus LockScheduler()
+        CRayStatus UnlockScheduler()

         unique_ptr[CProfileEvent] CreateProfileEvent(
             const c_string &event_type)
@@ -237,6 +240,7 @@ cdef extern from "ray/core_worker/core_worker.h" nogil:
         c_bool GetCurrentTaskRetryExceptions()
         CPlacementGroupID GetCurrentPlacementGroupId() const
         CWorkerID GetWorkerID()
+        CLockID GetSchedLockID()
         c_bool ShouldCaptureChildTasksInPlacementGroup()
         CActorID GetActorId() const
         const c_string GetActorName()
diff --git a/python/ray/includes/unique_ids.pxd b/python/ray/includes/unique_ids.pxd
index fe93a83675..7bd83afb20 100644
--- a/python/ray/includes/unique_ids.pxd
+++ b/python/ray/includes/unique_ids.pxd
@@ -188,4 +188,19 @@ cdef extern from "ray/common/id.h" namespace "ray" nogil:
         @staticmethod
         CPlacementGroupID Of(CJobID job_id)

+    cdef cppclass CLockID "ray::LockID" \
+                                    (CBaseID[CLockID]):
+
+        @staticmethod
+        CLockID FromBinary(const c_string &binary)
+
+        @staticmethod
+        CLockID FromHex(const c_string &hex_str)
+
+        @staticmethod
+        const CLockID Nil()
+
+        @staticmethod
+        size_t Size()
+
     ctypedef uint32_t ObjectIDIndexType
diff --git a/python/ray/includes/unique_ids.pxi b/python/ray/includes/unique_ids.pxi
index db107d88a7..3e020179f0 100644
--- a/python/ray/includes/unique_ids.pxi
+++ b/python/ray/includes/unique_ids.pxi
@@ -20,6 +20,7 @@ from ray.includes.unique_ids cimport (
     CWorkerID,
     CPlacementGroupID,
     CClusterID,
+    CLockID,
 )


diff --git a/python/ray/util/placement_group.py b/python/ray/util/placement_group.py
index cd9ac6fa2b..1f20a2baf2 100644
--- a/python/ray/util/placement_group.py
+++ b/python/ray/util/placement_group.py
@@ -682,3 +682,17 @@ def _configure_placement_group_based_on_context(
             placement_group, resources, placement_resources, task_or_actor_repr
         )
     return placement_group
+
+@PublicAPI
+@client_mode_wrap
+def lock_scheduler():
+    worker = ray._private.worker.global_worker
+    worker.check_connected()
+    return worker.core_worker.lock_scheduler()
+
+@PublicAPI
+@client_mode_wrap
+def unlock_scheduler():
+    worker = ray._private.worker.global_worker
+    worker.check_connected()
+    return worker.core_worker.unlock_scheduler()
diff --git a/src/ray/common/id.cc b/src/ray/common/id.cc
index 9883ef0c26..539e9973ad 100644
--- a/src/ray/common/id.cc
+++ b/src/ray/common/id.cc
@@ -344,7 +344,10 @@ ID_OSTREAM_OPERATOR(TaskID);
 ID_OSTREAM_OPERATOR(ObjectID);
 ID_OSTREAM_OPERATOR(PlacementGroupID);
 ID_OSTREAM_OPERATOR(LeaseID);
+ID_OSTREAM_OPERATOR(LockID);

 const NodeID kGCSNodeID = NodeID::FromBinary(std::string(kUniqueIDSize, 0));
+const LockID kSchedLockID =
+LockID::FromHex("534348444C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C4C"); // SCHD

 }  // namespace ray
diff --git a/src/ray/common/id.h b/src/ray/common/id.h
index 8e89d7e55c..b8575a7584 100644
--- a/src/ray/common/id.h
+++ b/src/ray/common/id.h
@@ -597,6 +597,11 @@ inline std::vector<ObjectID> ObjectRefsToIds(
   return object_ids;
 }

+template <>
+struct DefaultLogKey<LockID> {
+  constexpr static std::string_view key = kLogKeyLockID;
+};
+
 }  // namespace ray

 namespace std {
@@ -621,4 +626,5 @@ DEFINE_UNIQUE_ID(LeaseID);

 namespace ray {
 extern const NodeID kGCSNodeID;
+extern const LockID kSchedLockID;
 }
diff --git a/src/ray/common/id_def.h b/src/ray/common/id_def.h
index 360c44dd7f..7ef088d9df 100644
--- a/src/ray/common/id_def.h
+++ b/src/ray/common/id_def.h
@@ -24,3 +24,4 @@ DEFINE_UNIQUE_ID(WorkerID)
 DEFINE_UNIQUE_ID(ConfigID)
 DEFINE_UNIQUE_ID(NodeID)
 DEFINE_UNIQUE_ID(ClusterID)
+DEFINE_UNIQUE_ID(LockID)
diff --git a/src/ray/core_worker/context.cc b/src/ray/core_worker/context.cc
index 35b80bfe4a..27f4e5aced 100644
--- a/src/ray/core_worker/context.cc
+++ b/src/ray/core_worker/context.cc
@@ -181,6 +181,7 @@ WorkerContext::WorkerContext(WorkerType worker_type,
 WorkerType WorkerContext::GetWorkerType() const { return worker_type_; }

 const WorkerID &WorkerContext::GetWorkerID() const { return worker_id_; }
+const LockID &WorkerContext::GetSchedLockID() const { return kSchedLockID; }

 uint64_t WorkerContext::GetNextTaskIndex() {
   return GetThreadContext().GetNextTaskIndex();
diff --git a/src/ray/core_worker/context.h b/src/ray/core_worker/context.h
index bf35ef3e56..3120dd945d 100644
--- a/src/ray/core_worker/context.h
+++ b/src/ray/core_worker/context.h
@@ -58,6 +58,7 @@ class WorkerContext {
   WorkerType GetWorkerType() const;

   const WorkerID &GetWorkerID() const;
+  const LockID &GetSchedLockID() const;

   JobID GetCurrentJobID() const ABSL_LOCKS_EXCLUDED(mutex_);
   rpc::JobConfig GetCurrentJobConfig() const ABSL_LOCKS_EXCLUDED(mutex_);
diff --git a/src/ray/core_worker/core_worker.cc b/src/ray/core_worker/core_worker.cc
index 426bc5e08f..e1c24538f7 100644
--- a/src/ray/core_worker/core_worker.cc
+++ b/src/ray/core_worker/core_worker.cc
@@ -5083,4 +5083,21 @@ void CoreWorker::SendFreeLocalObjectsBatchIfNeeded(const NodeID &node_id) {
       });
 }

+Status CoreWorker::LockScheduler() {
+  return gcs_client_->PlacementGroups().TakeLock(
+      kSchedLockID,
+      worker_context_->GetCurrentJobID(),
+      GetCurrentNodeId(),
+      9999
+      );
+}
+
+Status CoreWorker::UnlockScheduler() {
+  return gcs_client_->PlacementGroups().ReleaseLock(
+      kSchedLockID,
+      worker_context_->GetCurrentJobID(),
+      GetCurrentNodeId()
+      );
+}
+
 }  // namespace ray::core
diff --git a/src/ray/core_worker/core_worker.h b/src/ray/core_worker/core_worker.h
index 7846c48caf..0b9d4ec243 100644
--- a/src/ray/core_worker/core_worker.h
+++ b/src/ray/core_worker/core_worker.h
@@ -1531,6 +1531,9 @@ class CoreWorker : public std::enable_shared_from_this<CoreWorker> {
   void FreeObjectOnNodesAsync(const ObjectID &object_id,
                               const absl::flat_hash_set<NodeID> &locations);

+  Status LockScheduler();
+  Status UnlockScheduler();
+
  private:
   /// Resolve a raylet RPC client by node id. Should be used to only get a temporary RPC
   /// client, since the retryable GRPC client relies on clients going out of scope to
diff --git a/src/ray/gcs/gcs_init_data.cc b/src/ray/gcs/gcs_init_data.cc
index b223a3bc2c..f8d98fe543 100644
--- a/src/ray/gcs/gcs_init_data.cc
+++ b/src/ray/gcs/gcs_init_data.cc
@@ -21,8 +21,8 @@
 namespace ray {
 namespace gcs {
 void GcsInitData::AsyncLoad(Postable<void()> on_done) {
-  // There are 6 kinds of table data need to be loaded.
-  auto count_down = std::make_shared<int>(6);
+  // There are 7 kinds of table data that need to be loaded.
+  auto count_down = std::make_shared<int>(7);
   auto on_load_finished = Postable<void()>(
       [count_down, on_done]() mutable {
         if (--(*count_down) == 0) {
@@ -42,6 +42,8 @@ void GcsInitData::AsyncLoad(Postable<void()> on_done) {
   AsyncLoadPlacementGroupTableData(on_load_finished);

   AsyncLoadWorkerTableData(on_load_finished);
+
+  AsyncLoadLockStatusTableData(on_load_finished);
 }

 void GcsInitData::AsyncLoadJobTableData(Postable<void()> on_done) {
@@ -105,5 +107,15 @@ void GcsInitData::AsyncLoadWorkerTableData(Postable<void()> on_done) {
       }));
 }

+void GcsInitData::AsyncLoadLockStatusTableData(Postable<void()> on_done) {
+  RAY_LOG(INFO) << "Loading lock status table data.";
+  gcs_table_storage_.LockStatusTable().GetAll(std::move(on_done).TransformArg(
+      [this](absl::flat_hash_map<LockID, rpc::LockStatus> result) {
+        lock_status_table_data_ = std::move(result);
+        RAY_LOG(INFO) << "Finished loading lock status table data, size = "
+                      << lock_status_table_data_.size();
+      }));
+}
+
 }  // namespace gcs
 }  // namespace ray
diff --git a/src/ray/gcs/gcs_init_data.h b/src/ray/gcs/gcs_init_data.h
index 819246773d..c780fa3f02 100644
--- a/src/ray/gcs/gcs_init_data.h
+++ b/src/ray/gcs/gcs_init_data.h
@@ -74,6 +74,15 @@ class GcsInitData {
     return worker_table_data_;
   }

+  /**
+   * @brief Get the lock status metadata loaded from the lock status table.
+   *
+   * @return Map from lock id to its lock status table entry.
+   */
+  const absl::flat_hash_map<LockID, rpc::LockStatus> &LockStatus() const {
+    return lock_status_table_data_;
+  }
+
  private:
   /// Load job metadata from the store into memory asynchronously.
   ///
@@ -104,6 +113,13 @@ class GcsInitData {
    */
   void AsyncLoadWorkerTableData(Postable<void()> on_done);

+  /**
+   * @brief Load lock status metadata from the store into memory asynchronously.
+   *
+   * @param on_done The callback invoked when lock status metadata is loaded successfully.
+   */
+  void AsyncLoadLockStatusTableData(Postable<void()> on_done);
+
  protected:
   /// The gcs table storage.
   gcs::GcsTableStorage &gcs_table_storage_;
@@ -125,6 +141,9 @@ class GcsInitData {

   /// Worker metadata.
   absl::flat_hash_map<WorkerID, rpc::WorkerTableData> worker_table_data_;
+
+  /// Lock status metadata.
+  absl::flat_hash_map<LockID, rpc::LockStatus> lock_status_table_data_;
 };

 }  // namespace gcs
diff --git a/src/ray/gcs/gcs_leader_gated_handlers.h b/src/ray/gcs/gcs_leader_gated_handlers.h
index 122cc17a0a..da863be84e 100644
--- a/src/ray/gcs/gcs_leader_gated_handlers.h
+++ b/src/ray/gcs/gcs_leader_gated_handlers.h
@@ -231,6 +231,12 @@ class LeaderGatedPlacementGroupInfoHandler
   GCS_GATED_RPC(HandleGetNamedPlacementGroup,
                 rpc::GetNamedPlacementGroupRequest,
                 rpc::GetNamedPlacementGroupReply)
+  GCS_GATED_RPC(HandleTakeLock,
+                rpc::TakeLockRequest,
+                rpc::TakeLockReply)
+  GCS_GATED_RPC(HandleReleaseLock,
+                rpc::ReleaseLockRequest,
+                rpc::ReleaseLockReply)

  private:
   rpc::PlacementGroupInfoGcsServiceHandler &handler_;
diff --git a/src/ray/gcs/gcs_placement_group_manager.cc b/src/ray/gcs/gcs_placement_group_manager.cc
index 8d1729f3b6..888cee75cd 100644
--- a/src/ray/gcs/gcs_placement_group_manager.cc
+++ b/src/ray/gcs/gcs_placement_group_manager.cc
@@ -757,6 +757,8 @@ void GcsPlacementGroupManager::OnNodeDead(const NodeID &node_id) {
       }
     }
   }
+
+  RemoveLocksByNode(node_id);
 }

 void GcsPlacementGroupManager::OnNodeAdd(const NodeID &node_id) {
@@ -800,6 +802,8 @@ void GcsPlacementGroupManager::CleanPlacementGroupIfNeededWhenJobDead(
       }
     });
   }
+
+  RemoveLocksByJob(job_id);
 }

 void GcsPlacementGroupManager::CleanPlacementGroupIfNeededWhenActorDead(
@@ -1087,5 +1091,96 @@ bool GcsPlacementGroupManager::RescheduleIfStillHasUnplacedBundles(
   return false;
 }

+// TODO(sca): release lock when:
+// - owner node dies
+// - owner job dies
+//
+// TODO(sca): add table storage
+void GcsPlacementGroupManager::HandleTakeLock(
+    rpc::TakeLockRequest request,
+    rpc::TakeLockReply *reply,
+    rpc::SendReplyCallback send_reply_callback) {
+  absl::MutexLock lock(&lock_table_mutex_);
+
+  RAY_LOG(DEBUG) << "LOCKREQ " << request.DebugString();
+  auto lock_id = LockID::FromBinary(request.lock_id());
+
+  if (!locks_.contains(lock_id)) {
+    rpc::LockStatus lock_status;
+
+    lock_status.set_lock_id(request.lock_id());
+    lock_status.set_job_id(request.job_id());
+    lock_status.set_node_id(request.node_id());
+    lock_status.set_state(rpc::LockState::LOCKED);
+
+    locks_.emplace(lock_id, lock_status);
+    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
+    return;
+  }
+
+  auto iter = locks_.find(lock_id); // expected to be present based on above check
+  if (iter->second.job_id() == request.job_id() && iter->second.node_id() ==
+      request.node_id()) {
+    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
+    return;
+  }
+
+  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::AlreadyExists("locked"));
+}
+
+void GcsPlacementGroupManager::HandleReleaseLock(
+    rpc::ReleaseLockRequest request,
+    rpc::ReleaseLockReply *reply,
+    rpc::SendReplyCallback send_reply_callback) {
+  RAY_LOG(DEBUG) << "LOCKREL " << request.DebugString();
+  absl::MutexLock lock(&lock_table_mutex_);
+  auto lock_id = LockID::FromBinary(request.lock_id());
+
+  auto iter = locks_.find(lock_id);
+  if (iter == locks_.end()) {
+    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::NotFound("not locked"));
+    return;
+  }
+
+  if (iter->second.job_id() == request.job_id() && iter->second.node_id() ==
+      request.node_id()) {
+    locks_.erase(lock_id);
+    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
+    return;
+  }
+
+  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::AlreadyExists("locked"));
+}
+
+void GcsPlacementGroupManager::RemoveLocksByNode(const NodeID &node_id) {
+  std::vector<LockID> removed;
+  absl::MutexLock lock(&lock_table_mutex_);
+  for (const auto &[lock_id, lock_status] : locks_) {
+    if (NodeID::FromBinary(lock_status.node_id()) == node_id) {
+      RAY_LOG(DEBUG).WithField(lock_id).WithField(node_id) << "NODEREL " << lock_status.DebugString();
+      removed.push_back(lock_id);
+    }
+  }
+
+  for (const auto &lock_id : removed) {
+    locks_.erase(lock_id);
+  }
+}
+
+void GcsPlacementGroupManager::RemoveLocksByJob(const JobID &job_id) {
+  std::vector<LockID> removed;
+  absl::MutexLock lock(&lock_table_mutex_);
+  for (const auto &[lock_id, lock_status] : locks_) {
+    if (JobID::FromBinary(lock_status.job_id()) == job_id) {
+      RAY_LOG(DEBUG).WithField(lock_id).WithField(job_id) << "JOBREL " << lock_status.DebugString();
+      removed.push_back(lock_id);
+    }
+  }
+
+  for (const auto &lock_id : removed) {
+    locks_.erase(lock_id);
+  }
+}
+
 }  // namespace gcs
 }  // namespace ray
diff --git a/src/ray/gcs/gcs_placement_group_manager.h b/src/ray/gcs/gcs_placement_group_manager.h
index 00b7b90256..4b0ac0417c 100644
--- a/src/ray/gcs/gcs_placement_group_manager.h
+++ b/src/ray/gcs/gcs_placement_group_manager.h
@@ -97,6 +97,17 @@ class GcsPlacementGroupManager : public rpc::PlacementGroupInfoGcsServiceHandler
       rpc::WaitPlacementGroupUntilReadyReply *reply,
       rpc::SendReplyCallback send_reply_callback) override;

+  void HandleTakeLock(rpc::TakeLockRequest request,
+                                  rpc::TakeLockReply *reply,
+                                  rpc::SendReplyCallback send_reply_callback) override;
+
+  void HandleReleaseLock(rpc::ReleaseLockRequest request,
+                                  rpc::ReleaseLockReply *reply,
+                                  rpc::SendReplyCallback send_reply_callback) override;
+
+  void RemoveLocksByJob(const JobID &job_id);
+  void RemoveLocksByNode(const NodeID &node_id);
+
   /// Register a callback which will be invoked after successfully created.
   ///
   /// \\param placement_group_id The placement group id which we want to listen.
@@ -378,6 +389,9 @@ class GcsPlacementGroupManager : public rpc::PlacementGroupInfoGcsServiceHandler
   ray::observability::MetricInterface &placement_group_count_gauge_;
   ClockInterface &clock_;

+  absl::Mutex lock_table_mutex_;
+  absl::flat_hash_map<LockID, rpc::LockStatus> locks_  ABSL_GUARDED_BY(lock_table_mutex_);
+
   FRIEND_TEST(GcsPlacementGroupManagerMockTest, PendingQueuePriorityReschedule);
   FRIEND_TEST(GcsPlacementGroupManagerMockTest, PendingQueuePriorityFailed);
   FRIEND_TEST(GcsPlacementGroupManagerMockTest, PendingQueuePriorityOrder);
diff --git a/src/ray/gcs/gcs_table_storage.cc b/src/ray/gcs/gcs_table_storage.cc
index cdba90f607..054500788a 100644
--- a/src/ray/gcs/gcs_table_storage.cc
+++ b/src/ray/gcs/gcs_table_storage.cc
@@ -208,6 +208,7 @@ template class GcsTable<ActorID, rpc::TaskSpec>;
 template class GcsTableWithJobId<ActorID, rpc::ActorTableData>;
 template class GcsTableWithJobId<ActorID, rpc::TaskSpec>;
 template class GcsTable<PlacementGroupID, rpc::PlacementGroupTableData>;
+template class GcsTable<LockID, rpc::LockStatus>;

 }  // namespace gcs
 }  // namespace ray
diff --git a/src/ray/gcs/gcs_table_storage.h b/src/ray/gcs/gcs_table_storage.h
index 46f1e8b746..e8c9c485da 100644
--- a/src/ray/gcs/gcs_table_storage.h
+++ b/src/ray/gcs/gcs_table_storage.h
@@ -197,6 +197,14 @@ class GcsWorkerTable : public GcsTable<WorkerID, rpc::WorkerTableData> {
   }
 };

+class GcsLockStatusTable : public GcsTable<LockID, rpc::LockStatus> {
+ public:
+  explicit GcsLockStatusTable(std::shared_ptr<StoreClient> store_client)
+      : GcsTable(std::move(store_client)) {
+    table_name_ = rpc::TablePrefix_Name(rpc::TablePrefix::LOCK_STATUS);
+  }
+};
+
 class GcsTableStorage {
  public:
   explicit GcsTableStorage(std::shared_ptr<StoreClient> store_client)
@@ -207,6 +215,7 @@ class GcsTableStorage {
     placement_group_table_ = std::make_unique<GcsPlacementGroupTable>(store_client_);
     node_table_ = std::make_unique<GcsNodeTable>(store_client_);
     worker_table_ = std::make_unique<GcsWorkerTable>(store_client_);
+    lock_status_table_ = std::make_unique<GcsLockStatusTable>(store_client_);
   }

   virtual ~GcsTableStorage() = default;
@@ -241,6 +250,11 @@ class GcsTableStorage {
     return *worker_table_;
   }

+  GcsLockStatusTable &LockStatusTable() {
+    RAY_CHECK(lock_status_table_ != nullptr);
+    return *lock_status_table_;
+  }
+
   void AsyncGetNextJobID(Postable<void(int)> callback) {
     RAY_CHECK(store_client_);
     store_client_->AsyncGetNextJobID(std::move(callback));
@@ -254,6 +268,7 @@ class GcsTableStorage {
   std::unique_ptr<GcsPlacementGroupTable> placement_group_table_;
   std::unique_ptr<GcsNodeTable> node_table_;
   std::unique_ptr<GcsWorkerTable> worker_table_;
+  std::unique_ptr<GcsLockStatusTable> lock_status_table_;
 };

 }  // namespace gcs
diff --git a/src/ray/gcs/grpc_service_interfaces.h b/src/ray/gcs/grpc_service_interfaces.h
index 93177b9f21..854442ce9b 100644
--- a/src/ray/gcs/grpc_service_interfaces.h
+++ b/src/ray/gcs/grpc_service_interfaces.h
@@ -296,6 +296,14 @@ class PlacementGroupInfoGcsServiceHandler {
   virtual void HandleGetNamedPlacementGroup(GetNamedPlacementGroupRequest request,
                                             GetNamedPlacementGroupReply *reply,
                                             SendReplyCallback send_reply_callback) = 0;
+
+  virtual void HandleTakeLock(TakeLockRequest request,
+                                            TakeLockReply *reply,
+                                            SendReplyCallback send_reply_callback) = 0;
+
+  virtual void HandleReleaseLock(ReleaseLockRequest request,
+                                            ReleaseLockReply *reply,
+                                            SendReplyCallback send_reply_callback) = 0;
 };

 namespace autoscaler {
diff --git a/src/ray/gcs/grpc_services.cc b/src/ray/gcs/grpc_services.cc
index 90aefc3ddf..eeceb3ef53 100644
--- a/src/ray/gcs/grpc_services.cc
+++ b/src/ray/gcs/grpc_services.cc
@@ -195,6 +195,10 @@ void PlacementGroupInfoGrpcService::InitServerCallFactories(
       PlacementGroupInfoGcsService, GetNamedPlacementGroup, max_active_rpcs_per_handler_)
   RPC_SERVICE_HANDLER(
       PlacementGroupInfoGcsService, GetAllPlacementGroup, max_active_rpcs_per_handler_)
+  RPC_SERVICE_HANDLER(
+      PlacementGroupInfoGcsService, TakeLock, max_active_rpcs_per_handler_)
+  RPC_SERVICE_HANDLER(
+      PlacementGroupInfoGcsService, ReleaseLock, max_active_rpcs_per_handler_)
   // WaitPlacementGroupUntilReady (pg.ready()) must stay uncapped (-1). Otherwise,
   // We may have a deadlock on it since the PG scheduling order may differ from the client
   // pg.ready() order. Example: cap=2, client waits for PG A, B, C to be ready. Only
diff --git a/src/ray/gcs_rpc_client/accessor.cc b/src/ray/gcs_rpc_client/accessor.cc
index f81bd6e6f4..f2edfa58f7 100644
--- a/src/ray/gcs_rpc_client/accessor.cc
+++ b/src/ray/gcs_rpc_client/accessor.cc
@@ -826,6 +826,34 @@ Status PlacementGroupInfoAccessor::SyncRemovePlacementGroup(
   return status;
 }

+Status PlacementGroupInfoAccessor::TakeLock(
+    const ray::LockID &lock_id, const ray::JobID &job_id,
+    const ray::NodeID &node_id, int64_t timeout_seconds) {
+  rpc::TakeLockRequest request;
+  rpc::TakeLockReply reply;
+  request.set_lock_id(lock_id.Binary());
+  request.set_job_id(job_id.Binary());
+  request.set_node_id(node_id.Binary());
+  request.set_op(rpc::LockOperation::LOCK);
+  auto status = client_impl_->GetGcsRpcClient().SyncTakeLock(
+      std::move(request), &reply, rpc::GetGcsTimeoutMs());
+  return status;
+}
+
+Status PlacementGroupInfoAccessor::ReleaseLock(
+    const ray::LockID &lock_id, const ray::JobID &job_id,
+    const ray::NodeID &node_id) {
+  rpc::ReleaseLockRequest request;
+  rpc::ReleaseLockReply reply;
+  request.set_lock_id(lock_id.Binary());
+  request.set_job_id(job_id.Binary());
+  request.set_node_id(node_id.Binary());
+  request.set_op(rpc::LockOperation::UNLOCK);
+  auto status = client_impl_->GetGcsRpcClient().SyncReleaseLock(
+      std::move(request), &reply, rpc::GetGcsTimeoutMs());
+  return status;
+}
+
 void PlacementGroupInfoAccessor::AsyncGet(
     const PlacementGroupID &placement_group_id,
     const rpc::OptionalItemCallback<rpc::PlacementGroupTableData> &callback) {
diff --git a/src/ray/gcs_rpc_client/accessor.h b/src/ray/gcs_rpc_client/accessor.h
index e4717189ef..b0133e3582 100644
--- a/src/ray/gcs_rpc_client/accessor.h
+++ b/src/ray/gcs_rpc_client/accessor.h
@@ -600,6 +600,11 @@ class PlacementGroupInfoAccessor {
   virtual Status SyncWaitUntilReady(const PlacementGroupID &placement_group_id,
                                     int64_t timeout_seconds);

+  virtual Status TakeLock(const LockID &lock_id, const JobID &job_id, const NodeID
+      &node_id, int64_t timeout_seconds);
+  virtual Status ReleaseLock(const LockID &lock_id, const JobID &job_id, const NodeID
+      &node_id);
+
  private:
   GcsClient *client_impl_;
 };
diff --git a/src/ray/gcs_rpc_client/rpc_client.h b/src/ray/gcs_rpc_client/rpc_client.h
index 2af555a2a9..49ec02ed5b 100644
--- a/src/ray/gcs_rpc_client/rpc_client.h
+++ b/src/ray/gcs_rpc_client/rpc_client.h
@@ -481,6 +481,16 @@ class GcsRpcClient {
                              placement_group_info_grpc_client_,
                              /*method_timeout_ms*/ -1, )

+  VOID_GCS_RPC_CLIENT_METHOD(PlacementGroupInfoGcsService,
+                             TakeLock,
+                             placement_group_info_grpc_client_,
+                             /*method_timeout_ms*/ -1, )
+
+  VOID_GCS_RPC_CLIENT_METHOD(PlacementGroupInfoGcsService,
+                             ReleaseLock,
+                             placement_group_info_grpc_client_,
+                             /*method_timeout_ms*/ -1, )
+
   /// Operations for kv (Get, Put, Del, Exists)
   VOID_GCS_RPC_CLIENT_METHOD(InternalKVGcsService,
                              InternalKVGet,
diff --git a/src/ray/protobuf/gcs.proto b/src/ray/protobuf/gcs.proto
index 93993048ce..68d4d94431 100644
--- a/src/ray/protobuf/gcs.proto
+++ b/src/ray/protobuf/gcs.proto
@@ -43,6 +43,7 @@ enum TablePrefix {
   PLACEMENT_GROUP = 17;
   KV = 18;
   ACTOR_TASK_SPEC = 19;
+  LOCK_STATUS = 20;
 }

 // The channel that Add operations to the Table should be published on, if any.
@@ -744,4 +745,18 @@ message JobTableData {
   // Address of the driver that started this job.
   Address driver_address = 12;
 }
+
+enum LockState {
+  UNKNOWN = 0;
+  LOCKED = 1;
+  UNLOCKED = 2;
+};
+
+message LockStatus {
+  bytes lock_id = 1;
+  LockState state = 2;
+  bytes job_id = 3;  // debugging
+  bytes node_id = 4; // debugging
+};
+
 ///////////////////////////////////////////////////////////////////////////////
diff --git a/src/ray/protobuf/gcs_service.proto b/src/ray/protobuf/gcs_service.proto
index 8560a443f8..a67b79ee0b 100644
--- a/src/ray/protobuf/gcs_service.proto
+++ b/src/ray/protobuf/gcs_service.proto
@@ -499,6 +499,42 @@ message GetNamedPlacementGroupReply {
   PlacementGroupTableData placement_group_table_data = 2;
 }

+enum LockOperation {
+  NOOP = 0;
+  LOCK = 1;
+  UNLOCK = 2;
+}
+
+enum LockResult {
+  NO_CHANGE = 0;
+  GRANTED = 1;
+  RELEASED = 2;
+};
+
+message TakeLockRequest {
+  bytes lock_id = 1;
+  LockOperation op = 2;
+  bytes job_id = 3;  // debugging
+  bytes node_id = 4; // debugging
+};
+
+message ReleaseLockRequest {
+  bytes lock_id = 1;
+  LockOperation op = 2;
+  bytes job_id = 3;  // debugging
+  bytes node_id = 4; // debugging
+};
+
+message TakeLockReply {
+  GcsStatus status = 1;
+  LockResult result = 2;
+};
+
+message ReleaseLockReply {
+  GcsStatus status = 1;
+  LockResult result = 2;
+};
+
 // Service for placement group info access.
 service PlacementGroupInfoGcsService {
   // Create placement group via gcs service.
@@ -518,6 +554,9 @@ service PlacementGroupInfoGcsService {
   // Wait for placement group until ready.
   rpc WaitPlacementGroupUntilReady(WaitPlacementGroupUntilReadyRequest)
       returns (WaitPlacementGroupUntilReadyReply);
+  // TODO(sca): this doesn't belong here
+  rpc TakeLock(TakeLockRequest) returns (TakeLockReply);
+  rpc ReleaseLock(ReleaseLockRequest) returns (ReleaseLockReply);
 }

 ///////////////////////////////////////////////////////////////////////////////
diff --git a/src/ray/util/logging.h b/src/ray/util/logging.h
index 16fb0d385c..ea77277663 100644
--- a/src/ray/util/logging.h
+++ b/src/ray/util/logging.h
@@ -98,6 +98,7 @@ inline constexpr std::string_view kLogKeyTaskID = "task_id";
 inline constexpr std::string_view kLogKeyObjectID = "object_id";
 inline constexpr std::string_view kLogKeyPlacementGroupID = "placement_group_id";
 inline constexpr std::string_view kLogKeyLeaseID = "lease_id";
+inline constexpr std::string_view kLogKeyLockID = "lock_id";

 // Define your specialization DefaultLogKey<your_type>::key to get .WithField(t)
 // See src/ray/common/id.h
"""
