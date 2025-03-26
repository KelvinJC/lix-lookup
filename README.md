# lix-lookup
An Elixir program to read two CSV files:
- the first file contains the staff ID, names, and roles of staff within a specific region.
- the second file contains ID, email, and additional data fields for a larger pool of staff.
the program performs a lookup using staff IDs from the first file
to find their corresponding emails in the second file.
It then generates a new CSV file containing the staff ID, name, and email for the subset of staff.


### **Implementation Overview**

This program efficiently processes large CSV files by leveraging concurrency.
It uses multiple asynchronous processes to map staff data and perform lookups.

Step-by-Step Execution:
 - Streaming & Parallel Processing:
  - The main process streams data from a file in chunks.  
  - It spawns multiple asynchronous worker processes to handle each chunk.  
  - Each worker:  
    - Parses each line in the chunk into a key-value map.  
     - Sends the map to an **Agent process** for caching.  

 - Caching & Data Merging: 
  - The **Agent process** collects and merges all key-value maps.  
   - It maintains the complete dataset in memory for fast lookups.  

 - Performing Lookups Efficiently:
  - The main process streams data from a second file.  
  - Another set of worker processes handles these new chunks.  
   - Each worker queries the **Agent** to find matching staff emails.  

 - Storing & Exporting Results:
  - The **Agent** maintains a list of matched staff records.  
   - The main process retrieves this data and exports it to a CSV file.  


### Key Benefits: 
**Memory Efficient** – Streams data in chunks instead of loading everything into memory.  
**Highly Concurrent** – Uses async processes to speed up mapping and lookups.  
**Fast Lookups** – Cached data in an Agent ensures quick retrieval.  
**Scalable** – Can handle large datasets without blocking execution.  

