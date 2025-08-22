#include "dberror.h"

#define IF_NULL(val, error, message) \
  if (val == NULL) {                      \
    RC_message=message;                 \
    return error;                           \
  }
