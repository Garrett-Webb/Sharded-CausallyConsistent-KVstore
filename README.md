# CSE138_Assignment4
Private repository for CSE138 - Distributed Systems - Assignment 4

## CruzIDs
* `gswebb`
* `kahsieh`
* `raarora`

## Citations:
* `https://stackoverflow.com/questions/40950791/remove-quotes-from-string-in-python/40950987`
* Used to determine how to strip replica address strings that had double quotes around them
* `https://stackabuse.com/serving-files-with-pythons-simplehttpserver-module/`
* `https://docs.python.org/3/library/http.server.html`
* Used to develop the httpserver that was reused from assignment 2. Provided an example to 
follow for our own server
* `https://stackoverflow.com/questions/31371166/reading-json-from-simplehttpserver-post-data`
* Used to determine proper syntax to send/receive json for the assignment 1 and 2 httpservers. 
* `https://realpython.com/python-requests/#headers`
* Used to understand how headers are used and formatted in python requests.
* `https://www.kite.com/python/answers/how-to-check-if-a-list-contains-a-substring-in-python`
* Used to identify if a view operation is coming from an IP that belongs to a replica instead of a client.
* `https://www.w3schools.com/python/python_variables_global.asp`
* Used to learn how to modify global variables in python.
* `https://levelup.gitconnected.com/consistent-hashing-27636286a8a9`
* Used to determine how to have a consistent hashing function across multiple instances, as python's hash function uses a random seed for each server instance


## Team Contributions:
* `all together:`  We worked on the entire project together over a call with Visual Studio Code Live Share enabled. This allowed us to all look at and modify the same files. This included group debugging and talking over the ideas and implementation as a group. We alternated the roles of who would code while the others watched and helped. All work described below was done by the individual with the team supporting.

* `Garrett:` Developed majority of helper endpoints. With the help of my groupmates, I added the helper endpoints for the sharding operations, helper endpoints, and modified the keyvalue store functionality to operate in a sharded manner. Also helped with the documentation and creation of resharding mechanism.

* `Rahul:` Derived logic for reshard. Determined what data structures to use for storing shards and shard information. Developed vector clock logic. Coded alongside Garrett and Kai using VSCode Live Share. 

* `Kai:` Setup the testing/debugging routine and performed all of the testings. Started the initial key-value-store-view operation which built the base structure for rest of the request handling.


## Acknowledgements:
* `Vinay Venkat (tutor):` Helped us with Assignment 3, which was used as a base for Assignment 4. Provided us with a basic overview of a potential way to use vector clocks within our environment during his tutoring session. 