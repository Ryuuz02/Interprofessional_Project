#include "dberror.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

char *RC_message;

/* print a message to standard out describing the error */
void printError(RC error) {
    if (RC_message != NULL) {
        printf("EC (%i), \"%s\"\n", error, RC_message);
    } else {
        printf("EC (%i)\n", error);
    }
}

char *errorMessage(RC error) {
    char *message;
    size_t message_size;

    if (RC_message != NULL) {
        message_size = strlen(RC_message) + 30;
        message = (char *) malloc(message_size);

        snprintf(message, message_size, "EC (%i), \"%s\"\n", error, RC_message);
    } else {
        message_size = 30;
        message = (char *) malloc(message_size);

        snprintf(message, message_size, "EC (%i)\n", error);
    }

    return message;
}
