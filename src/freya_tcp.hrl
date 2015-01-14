-define(DEFAULT_PORT, 7055).
-define(DEFAULT_CODEC, freya_tcp_codec).

-define(HDR            , 16#FF).
-define(OPCODE_VERSION , $v).
-define(OPCODE_PUT     , $p).
-define(OPCODE_STATUS  , $s).


-record(proto, { codec = ?DEFAULT_CODEC :: module(),
                 socket                 :: inet:socket(),
                 transport              :: any(),
                 publisher              :: eqm:pub(),
                 version                :: iodata() 
               }).
