# lix-lookup
An Elixir program to read two CSV files:
- the first file contains the staff ID, names, and roles of staff within a specific region.
- the second file contains ID, email, and additional data fields for a larger pool of staff.
the program performs a lookup using staff IDs from the first file
to find their corresponding emails in the second file.
It then generates a new CSV file containing the staff ID, name, and email for the subset of staff.


## Implementation Overview

### Creating and Tracking Processes
The main process creates an agent process called `StaffCacheRegister`, which is responsible for generating and tracking multiple agent processes to serve as memory caches.

### Streaming & Parallel Processing
The main process streams rows of data from a file and spawns multiple asynchronous processes. Each process is responsible for:
- Receiving and parsing rows of staff data.
- Constructing a key-value map from the parsed rows.
- Querying the `StaffCacheRegister` for the PID of a `StaffCache` process.
- Sending the map to the `StaffCache` process for caching.

The main process then streams lines of data from a second file and spawns another batch of asynchronous processes. Each of these processes is responsible for:
- Receiving rows of streamed staff data.
- Querying each `StaffCache` agent process to match staff with their emails.

### Caching & Data Merging
Each `StaffCache` process receives and stores parsed staff data in its internal state. As new data is streamed, these processes update their internal key-value maps with the new information, ensuring efficient data merging and retrieval.

### Performing Lookups Efficiently
Each `StaffCache` process matches staff to their email records by performing lookups of each line against the key-value map stored in its internal state. The process maintains a list of matched staff records for efficient retrieval.

### Storing & Exporting Results
The main process retrieves the matched data from all `StaffCache` processes and consolidates the results. The final dataset is then exported to a CSV file for further analysis or external use.



## Key Benefits: 
**Memory Efficient** – Streams data in chunks instead of loading everything into memory.  
**Highly Concurrent** – Uses async processes to speed up mapping and lookups.  
**Fast Lookups** – Cached data in Agents ensures quick retrieval.  
**Scalable** – Can handle large datasets without blocking execution.  
