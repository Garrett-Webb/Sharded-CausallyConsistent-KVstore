# CSE138_Assignment3
Private repository for CSE138 - Distributed Systems - Assignment 3

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


## Team Contributions:
* `all together:` We worked on the entire project together over a call with Visual Studio Code Live Share enabled. This allowed us to all look at and modify the same files. This included group debugging and talking over the ideas and implementation as a group. We alternated the roles of who would code while the others watched and helped. All work described below was done by the individual with the team supporting.

* `Garrett:` With the help of my groupmates, I added the helper endpoints for Vector Clock and Key Value update functionality. Also added helper endpoints for server functionality to PUT, GET, and DELETE without broadcasting to other instances as terminal cases. Built most of the basic functionality for the previous assignment that was reused here.

* `Rahul:` Designed the vector clock implementation that we used. Originally demonstrated potential use cases with drawn diagrams to determine when vector clocks should be incremented, passed back to clients, or requested to be updated from other replicas. The implementation was done with the help of my groupmates. 

* `Kai:` Setup the testing/debugging routine and performed most of the testings. Started the initial key-value-store-view operation which built the base structure for rest of the request handling.


## Acknowledgements:
* `Vinay Venkat (tutor):` Provided us with a basic overview of a potential way to use vector clocks within our environment during his tutoring session.