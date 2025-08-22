#ifndef DT_H
#define DT_H

// Check if 'bool' is not defined yet
#ifndef bool
    typedef short bool;  // Define 'bool' as 'short'
    #define true 1       // Define 'true' as 1
    #define false 0      // Define 'false' as 0
#endif

// Define TRUE and FALSE for uppercase compatibility
#define TRUE true
#define FALSE false

#endif // DT_H