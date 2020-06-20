module Names =
struct
  (* Metrics reported by workers: *)
  let in_tuple_count = "in_tuple_count"
  let selected_tuple_count = "selected_tuple_count"
  let out_tuple_count = "out_tuple_count"
  let firing_notif_count = "firing_notif_count"
  let extinguished_notif_count = "extinguished_notif_count"
  let group_count = "group_count"
  let cpu_time = "cpu_time"
  let ram_usage = "ram_usage"
  let ram_usage = "ram_usage"
  let rb_wait_read = "in_sleep"
  let rb_wait_write = "out_sleep"
  let worker_read_bytes = "in_bytes"
  let worker_write_bytes = "out_bytes"
  let last_out = "last_out"
  let event_time = "event_time"
  let avg_full_out_bytes = "avg_full_out_bytes"
  let relocated_groups = "relocated_groups"
  let num_subscribers = "subscribers"
  let num_sync_msgs_in = "sync_msgs_in"
  let num_sync_msgs_out = "sync_msgs_out"
  let sync_resp_time = "sync_resp_time"
  let num_rate_limited_unpublished = "rate_limited_unpublished"
  let perf_per_tuple = "perf_per_tuple"
  let perf_where_fast = "perf_where_fast"
  let perf_find_group = "perf_find_group"
  let perf_where_slow = "perf_where_slow"
  let perf_update_group = "perf_update_group"
  let perf_commit_incoming = "perf_commit_incoming"
  let perf_select_others = "perf_select_others"
  let perf_finalize_others = "perf_finalize_others"
  let perf_commit_others = "perf_commit_others"
  let perf_flush_others = "perf_flush_others"

  (* Metrics reported by the supervisor: *)
  let worker_crashes = "workers_crashes"
  let worker_deadloopings = "workers_deadloopings"
  let worker_sigkills = "workers_sigkills"
  let ringbuf_repairs = "workers_ringbuf_repairs"
  let outref_repairs = "workers_outref_repairs"
  let replayer_crashes = "replayers_crashes"
  let replayer_sigkills = "replayers_sigkills"
  let chans_per_replayer = "channels_per_replayer"
  let worker_graph_build_time = "workers_graph_build_time"

  (* Metrics reported by the alerter: *)
  let messages_count = "messages_count"
  let messages_send_fails = "notifs_send_failures"
  let team_fallbacks = "notifs_team_fallbacks"
  let messages_cancelled = "messages_cancelled"

  (* Metrics reported by the HTTP server: *)
  let requests_count = "http_requests_count"
  let http_resp_time = "http_resp_time"

  (* Metrics reported by the compiler: *)
  let compiler_typing_time = "compiler_typing_time"
  let compiler_typing_count = "compiler_typing_count"

  (* Metrics reported by the copy service: *)
  let copy_server_accepts = "copy_server_accepts"
  let copy_server_tuples = "copy_server_tuples"
  let copy_client_connects = "copy_client_connects"
  let copy_client_tuples = "copy_client_tuples"

  (* Metrics reported by the confserver: *)
  let sync_session_count = "sync_sessions_count"
  let sync_user_count = "sync_users_count"
  let sync_subscription_count = "sync_subscriptions_count"
  let sync_sent_msgs = "sync_sent_msgs"
  let sync_sent_bytes = "sync_sent_bytes"
  let sync_recvd_msgs = "sync_recvd_msgs"
  let sync_recvd_bytes = "sync_recvd_bytes"
  let sync_bad_recvd_msgs = "sync_bad_recvd_msgs"
end

(* Only required when the doc is used in more than one place: *)
module Docs =
struct
  let in_tuple_count =
    "Number of received tuples since the worker has started (live channel \
     only)."
  let selected_tuple_count =
    "Number of received tuples that have passed the WHERE filter since \
     the worker has started (live channel only)."
  let out_tuple_count =
    "Number of tuples sent to each child of this worker since it \
     started (on live channel)."
  let firing_notif_count =
    "Number of notifications sent for a firing alert."
  let extinguished_notif_count =
    "Number of notifications sent for a no-longer firing alert."
  let group_count =
    "Number of groups currently maintained (for the live channel)."
  let cpu_time =
    "Total CPU time spent in this worker so far (regardless of the \
     channel)."
  let ram_usage =
    "Number of bytes currently allocated in the heap for that worker \
     (regardless of the channel)."
  let max_ram_usage =
    "Maximum observed value of ram_usage."
  let worker_read_bytes =
    "Number of bytes read from the input ring buffers so far (for the \
     live channel only)."
  let worker_write_bytes =
    "Number of bytes written in all output ring buffers or output socket \
     (for top-halves) since that worker has started (regardless of the \
     channel)."
  let rb_wait_read =
    "Total number of seconds spent waiting for the input to refill."
  let rb_wait_write =
    "Total number of seconds spent waiting for the output to empty."
  let last_out =
    "When was the last output emitted (for the live channel only)."
  let event_time =
    "Last, minimum and maximum (since startup) event time emitted \
     (for the live channel only)."
  let avg_full_out_bytes = "Average size of a fully-fledged out tuple."
  let relocated_groups =
    "How many times a group was moved in the commit precondition heap."
  let num_subscribers = "Number of tail-subscribers"
  let num_sync_msgs_in = "Number of received synchronisation messages"
  let num_sync_msgs_out = "Number of emitted synchronisation messages"
  let num_rate_limited_unpublished =
    "Number of tuples that were unpublished due to rate limit"
  let perf_per_tuple = "Average time spent processing an incoming tuples."
  let perf_where_fast =
    "Average time spent executing the (fast) WHERE clause for each \
     incoming tuple."
  let perf_find_group =
    "Average time spent looking for the group for each incoming tuple."
  let perf_where_slow =
    "Average time spent executing the (slow) WHERE clause for each \
     incoming tuple."
  let perf_update_group =
    "Average time spent updating the aggregate group for each incoming \
     tuple."
  let perf_commit_incoming =
    "Average time spent committing the incoming tuple."
  let perf_select_others =
    "Average time spent looking for other tuples to commit, for each \
     incoming one."
  let perf_finalize_others =
    "Average time spent finalizing the values to output for other tuples, \
     for each incoming one."
  let perf_commit_others =
    "Average time spent committing other tuples, for each \
     incoming one."
  let perf_flush_others =
    "Average time spent flushing other tuples, for each incoming one."
  let profile = "Performance measurements."
  let copy_client_connects = "Number of attempted connections."
  let copy_client_tuples = "Number of sent tuples."
end
