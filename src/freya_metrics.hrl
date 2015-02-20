-define(Q_DECODE_STREAM  , <<"freya.codec.decode_stream">>).
-define(Q_HANDLE_PACKETS , <<"freya.tcp.connection.handle_packets">>).
-define(Q_PUT_METRIC     , <<"freya.tcp.connection.put_metric">>).
-define(Q_WRITER_BLOB    , <<"freya.writer.blob">>).
-define(Q_WRITER_BATCH   , <<"freya.writer.batch">>).

-define(Q_VNODE_IN_MEMORY,          <<"freya.vnode.in_memory">>).
-define(Q_VNODE_GET_LAT,            <<"freya.vnode.get.latency">>).
-define(Q_VNODE_GET_OK,             <<"freya.vnode.get.OK">>).
-define(Q_VNODE_GET_TIMEOUT,        <<"freya.vnode.get.TIMEOUT">>).
-define(Q_VNODE_PUSH_LAT,           <<"freya.vnode.push.latency">>).
-define(Q_VNODE_PUSH_OK,            <<"freya.vnode.push.OK">>).
-define(Q_VNODE_PUSH_TIMEOUT,       <<"freya.vnode.push.TIMEOUT">>).
-define(Q_VNODE_SNAPSHOT_LATENCY,   <<"freya.vnode.snapshot.latency">>).

-define(Q_EDGE_ROLLUP_PROCS,    <<"freya.edge.processes">>).
-define(Q_EDGE_OUTDATED,        <<"freya.edge.outdated">>).
