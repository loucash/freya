-record(freya_object,
        {vclock     :: vclock:vclock(),
         metric,
         tags,
         ts,
         fn         :: avg | min | max | sum,
         precision,
         values     :: dict()}).

-type freya_object()    :: #freya_object{}.

-record(val, {value     :: number(),
              points    :: non_neg_integer()}).
