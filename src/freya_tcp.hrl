-define(DEFAULT_PORT, 7055).
-define(DEFAULT_CODEC, freya_tcp_codec).

-record(proto, { codec = ?DEFAULT_CODEC :: module(),
                 publisher = eqm:pub(),
                 version :: iodata() }).
