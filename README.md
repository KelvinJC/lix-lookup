# lix-lookup
An Elixir program to read two CSV files:
- the first file contains the staff ID, names, and roles of staff within a specific region.
- the second file contains ID, email, and additional data fields for a larger pool of staff.
the program performs a lookup using staff IDs from the first file
to find their corresponding emails in the second file.
It then generates two new CSV files:
- one containing the staff ID, name, and email for the subset of staff.
  if an email is not found, the email field will contain either "No email" or "Invalid email".
- and another containing the names of those staff whose email were not found


# **Implementation Overview**

This program efficiently processes large CSV files by leveraging concurrency.
It uses multiple asynchronous processes to map staff data and perform lookups.

Step-by-Step Execution:
- Read Staff Data:
  - The main process reads the first file as a stream.
  - It spawns multiple asynchronous processes to handle chunks of data.
 
- Parallel Data Mapping:
  - Each worker process:
    - Receives a chunk of streamed staff data.
    - Builds a key-value map from the received lines.
    - Transmits the generated map back to the main process.
 
- Data Merging & Caching:
  - The main process merges all received maps.
  - It caches the final merged map in an Agent for quick access.
 
- Read Second File & Perform Lookups:
  - A second batch of async processes reads another file.
  - Each process retrieves staff records and performs lookups against the cached data.
 
- Efficient Query Execution:
  - Lookups are performed by querying the Agent.
  - The system ensures minimal locking and maximizes performance.
 
## Key Benefits: 
**Memory Efficient** – Streams data in chunks instead of loading everything into memory.  
**Highly Concurrent** – Uses async processes to speed up mapping and lookups.  
**Fast Lookups** – Cached data in an Agent ensures quick retrieval.  
**Scalable** – Can handle large datasets without blocking execution.  

