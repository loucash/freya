-record(freya_object,
        {
         key        :: tuple(),
         metric     :: binary(),
         tags       :: orddict:orddict(),
         ts         :: non_neg_integer(),
         fn         :: avg | min | max | sum,
         precision  :: tuple(),

         values     :: dict(),

         vnode_vclock = vclock:fresh()  :: vclock:vclock(),
         cass_vclock  = vclock:fresh()  :: vclock:vclock()
        }).

-type freya_object()    :: #freya_object{}.

-record(val, {value     :: number(),
              points    :: non_neg_integer()}).
