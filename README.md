# DistributedHashTable
The project implemented the chord idea in distributed hash table
The code is written in Python 3. No need to intall any third party libraries. 
# Installation
You should have python 3 installed on your computer if not use commandline "pip install python 3"
Down the folder to your desired folder. And that's it.
# Run
Step 1: Navigate to the folder for the project and type "Python3 dht_node hostfile.txt line_number" to read the hostfile on certain line and initialize a dht server instance. Initialize the same amount of servers as the hostfile provided. 
Step 2: To added a new key value pair, type "python3 dht_client target_node_addr port key value" to store them.
Step 3: To update an existing key value pair, simply change the above command with a desire value and a existing key
Step 4: To delete a key value pair, simply omit the value for the key. 

