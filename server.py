# server.py

# Garrett Webb
# Kai Hsieh
# Rahul Arora

#Citations
#https://stackabuse.com/serving-files-with-pythons-simplehttpserver-module/
#https://docs.python.org/3/library/http.server.html
#https://stackoverflow.com/questions/31371166/reading-json-from-simplehttpserver-post-data
#https://realpython.com/python-requests/#headers
#https://www.kite.com/python/answers/how-to-check-if-a-list-contains-a-substring-in-python

import requests
import sys
import http.server
import socketserver
import json
from sys import argv
import os
kvstore = {}
vc = {}
main_flag = False
saddr = ""
views = ""
views_list = []

# dict contains all shards
# ID: [list of nodes in shardID]
shards = {}
# ID of shard that this belongs to
shardID = -1
shardCount = -1

# omega poggers hashing function
def magicHash(key, numshards):
    print("key is ", key)
    print("hashed shard is ", ((sum(bytearray(key.encode('utf-8'))) % numshards)+1) )
    print("This shard's ID is ", shardID)
    return int((sum(bytearray(key.encode('utf-8'))) % numshards)+1)

class requestHandler(http.server.BaseHTTPRequestHandler):
    def _set_headers(self, response_code):
        self.send_response(response_code)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    # function to distribute GET to other nodes in the same shard
    def distributeGET(self, keystr, insertShard, vc_str, data):
        print("placeholder")
        return

    # function to distribute PUT to other nodes in the same shard
    def distributePUT(self, keystr, insertShard, vc_str, data):
        for replica in views_list:
            if(replica in list(shards[insertShard])):
                if (replica != saddr):
                    try:
                        print("    Broadcasting PUT value ", str(keystr), " to ", str(replica))
                        r = requests.put('http://' + replica + "/broadcast-key-put/" + keystr, timeout=1, allow_redirects=False, headers=self.headers, json={"value" : data["value"], "causal-metadata":  vc_str})
                    except:
                        print("    The instance is down, broadcasting delete view to all up instances")
                        views_list.remove(replica)
                        for y in views_list:
                            print("    Broadcasting DELETE downed instance ", replica, "to ", y)
                            if (y != saddr) and (y != replica):
                                try:
                                    r = requests.delete('http://' + y + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : replica})
                                except:
                                    print("    instance is also down or busy")
        return

    # function to distribute DELETE to other nodes in the same shard
    def distributeDELETE(self, keystr, insertShard, vc_str, data):
        print("placeholder")
        return

    # if the server recieves a GET request, it is directed here
    def do_GET(self):
        global shardID
        global shardCount
        print("\n[+] recieved GET request from: " + str(self.client_address[0]) + " to path: " + str(self.path) + "\n") 

        # Shard GET operations
        if "/key-value-store-shard/shard-ids" in str(self.path):
            shard_ids = list(shards.keys())
            print("shard ids are ", shard_ids)
            self._set_headers(response_code=200)
            response = bytes(json.dumps({"message": "Shard IDs retrieved successfully", "shard-ids": shard_ids}), 'utf-8')
            self.wfile.write(response)

        elif "/key-value-store-shard/node-shard-id" in str(self.path):
            self._set_headers(response_code=200)
            response = bytes(json.dumps({"message": "Shard ID of the node retrieved successfully", "shard-id": shardID}), 'utf-8')
            self.wfile.write(response)

        elif "/key-value-store-shard/shard-id-members/" in str(self.path):
            shardID_str = str(self.path).split("shard-id-members/",1)[1]
            returndict = shards[int(shardID_str)]
            print("returning dict:", returndict )
            self._set_headers(response_code=200)
            response = bytes(json.dumps({"message": "Members of shard ID retrieved successfully", "shard-id-members": returndict}), 'utf-8')
            self.wfile.write(response)

        elif "/key-value-store-shard/shard-id-key-count/" in str(self.path):
            #if shardID matches ours, use it, if not forward it to the correct shard and return to client.
            insertShard = int(str(self.path).split("shard-id-key-count/",1)[1])
            
            # If the shard count is requested of a shard that we are not in
            if(shardID != insertShard):
                print("This shard does not have this key. forwarding to a node in the correct shard")
                # grab last server in shard insertShard
                
                # forward request to proper shard
                inserted = False
                index = 0
                while( (inserted == False) and (index < len(shards[insertShard])) ):
                    node = (shards[insertShard])[index]
                    try:
                        if node != saddr:
                            print("    Sending to node: ", node)
                            tempAddr = "http://" + str(node) + "/key-value-store-shard/shard-id-key-count/" + str(insertShard)
                            print(tempAddr)
                            r = requests.get(tempAddr, timeout=2, allow_redirects=False)
                            inserted = True
                            #forward response from other node to client
                            self._set_headers(r.status_code)
                            self.wfile.write(r.content)
                            break
                    except:
                        x = node
                        print("    EXCEPT: broadcasting DELETE view ", x)
                        views_list.remove(x)
                        for y in views_list:
                            print("    Broadcasting DELETE downed instance ", x, "to ", y)
                            if (y != saddr) and (y != x):
                                try:
                                    r = requests.delete('http://' + y + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : x})
                                except:
                                    print("    broadcast instance is down or busy")
                    index += 1
                #Failed insert
                if(inserted == False):
                    self._set_headers(response_code=500)
                    response = bytes(json.dumps({'error' : "Shard is down"}), 'utf-8')
                    self.wfile.write(response)
            # if the requested shard is the shard we are in
            else:
                self._set_headers(response_code=200)
                response = bytes(json.dumps({"message": "Key count of shard ID retrieved successfully", "shard-id-key-count": len(kvstore)}), 'utf-8')
                self.wfile.write(response)

        # VIEW operations
        elif "/update-vc-store" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self._set_headers(response_code=200)
            response = bytes(json.dumps(vc), 'utf-8')
            self.wfile.write(response)
        
        elif "/update-shard-dict" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self._set_headers(response_code=200)
            response = bytes(json.dumps(shards), 'utf-8')
            self.wfile.write(response)

        elif "/checkview/" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self._set_headers(response_code=200)
            response = bytes(json.dumps(vc), 'utf-8')
            self.wfile.write(response)

        elif "/key-value-store-view" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            down_instances = []
            for x in views_list:
                if x != saddr:
                    try:
                        # send a dummy get request to each instance in the view, dont care about response as long as it returns something
                        r = requests.get('http://' + x + "/checkview/", timeout=1, headers=self.headers)
                    except:
                        # if the dummy request errors, that means the instance is down. add it to list and remove it from the local views list.
                        down_instances.append(x)
                        views_list.remove(x)

            #broadcast view delete of down instances to the ones who arent down
            for x in views_list:
                if (x not in down_instances) and (x != saddr):
                    for y in down_instances:
                        try:
                            # broadcast a view delete to each downed instance
                            r = requests.delete('http://' + x + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : y})
                        except:
                            print("instance ", y, " is either down or busy")
            
    
            print("Views list (to send) is:", views_list)
            
            #send response
            self._set_headers(response_code=200)
            response = bytes(json.dumps({"message" : "View retrieved successfully", "view" : ','.join(views_list), "causal-metadata":"" }), 'utf-8')
            self.wfile.write(response)
            
        elif "/update-kv-store" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self._set_headers(response_code=200)
            response = bytes(json.dumps(kvstore), 'utf-8')
            self.wfile.write(response)
        
        elif "/key-value-store/" in str(self.path):
            vc_str = json.dumps(vc)
            print("GET: vc is: ", vc)
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)

            #TODO hash incoming thing and broadcast GET to correct shard


            ###########################################################################################################
            keystr = str(self.path).split("/key-value-store/",1)[1]

            insertShard = magicHash(keystr, shardCount)
            # if this key belongs in a different shard
            if(shardID != insertShard):
                print("This shard does not have this key. forwarding to a node in the correct shard")
                # grab last server in shard insertShard
                
                # forward request to proper shard
                inserted = False
                index = 0
                while( (inserted == False) and (index < len(shards[insertShard])) ):
                    node = (shards[insertShard])[index]
                    try:
                        print("    Trying: broadcast the GET to the correct shard at ", node)
                        r = requests.get('http://' + node + "/key-value-store/" + keystr, timeout=1, allow_redirects=True, json=data)
                        inserted = True
                        #forward response from other node to client
                        self._set_headers(r.status_code)
                        self.wfile.write(r.content)

                    except Exception as e: 
                        print("Exception was: ")
                        print(e)
                        x = node
                        print("    EXCEPT: broadcasting DELETE view ", x)
                        views_list.remove(x)
                        for y in views_list:
                            print("    Broadcasting DELETE downed instance ", x, "to ", y)
                            if (y != saddr) and (y != x):
                                try:
                                    r = requests.delete('http://' + y + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : x})
                                except:
                                    print("    broadcast instance is down or busy")
                    index += 1
                #Failed insert
                if(inserted == False):
                    self._set_headers(response_code=500)
                    response = bytes(json.dumps({'error' : "Shard containing key is down", 'message' : "Error in PUT", "causal-metadata":vc_str}), 'utf-8')
                    self.wfile.write(response)

            # if the key belongs in THIS shard
            else:
                try:
                    vc_temp = json.loads(data["causal-metadata"])
                except:
                    vc_temp = ""
                if vc_temp != "":
                    for x in vc_temp:
                        #print("vc_temp[",x ,"] is ", str(vc_temp[x]))
                        #print("VC[",x,"] ", " is ", str(vc[x]))
                            if vc_temp[str(x)] > vc[str(x)]:
                                #print("element is bigger kekw")
                                for replica in views_list:
                                    if (replica != saddr) and (replica in list(shards[insertShard])):
                                        try:
                                            r = requests.get('http://'+ replica + "/update-kv-store", timeout=1)
                                            response_json = r.json()
                                            print(type(response_json))
                                            for key in response_json:
                                                kvstore[key] = response_json[key]

                                            r = requests.get('http://'+ replica + "/update-vc-store", timeout=1)
                                            response_json = r.json()
                                            print(type(response_json))
                                            for key in response_json:
                                                vc[str(key)] = max(vc[str(key)],response_json[key])
                                        except:
                                            print("we have failed")
                                        try:
                                            r = requests.put('http://' + replica + "/broadcast-view-put", timeout=1, allow_redirects=False, json={"socket-address" : saddr})
                                        except:
                                            print("replica ", replica, " in view is not yet live.")
                                        break
                    
                #print("BROADCAST GET causal metadata")
                #print(vc)
                # keystr = str(self.path).split("/key-value-store/",1)[1]
                if(len(keystr) > 0 and len(keystr) < 50):
                    if keystr in kvstore:
                        self._set_headers(response_code=200)
                        response = bytes(json.dumps({"doesExist" : True, "message" : "Retrieved successfully", "value" : kvstore[keystr], "causal-metadata":vc_str}), 'utf-8')
                    else:
                        self._set_headers(response_code=404)
                        response = bytes(json.dumps({"doesExist" : False, "error" : "Key does not exist", "message" : "Error in GET", "causal-metadata":vc_str}), 'utf-8')
                elif (len(keystr) > 50):
                    self._set_headers(response_code=400)
                    response = bytes(json.dumps({'error' : "Key is too long", 'message' : "Error in GET", "causal-metadata":vc_str}), 'utf-8')
                elif(len(keystr) == 0):
                    self._set_headers(response_code=400)
                    response = bytes(json.dumps({'error' : "Key not specified", 'message' : "Error in GET", "causal-metadata":vc_str}), 'utf-8')
                self.wfile.write(response)
        
        else:
            #default 500 code to clean up loose ends
            self._set_headers(response_code=500)

        return

    # if the server recieves a PUT request, it is directed here
    def do_PUT(self):
        global shardID
        global shardCount
        print("\n[+] recieved PUT request from: " + str(self.client_address[0]) + " to path: " + str(self.path) + "\n")

        # Shard PUT operations
        if "/key-value-store-shard/reshard" in str(self.path):
            # reshard the kvstore
            try:
                self.data_string = self.rfile.read(int(self.headers['Content-Length']))
                data = json.loads(self.data_string)
                new_shard_count = int(data["shard-count"])
            except Exception as e:
                print("exception was: ", e)
                new_shard_count = -1

            print("The shard count is", new_shard_count)
            
            if len(views_list) / int(new_shard_count) >= 2:
                # get all key/values in one place
                print("Kvstore (before)", kvstore)
                for shardKey in shards.keys():
                    shardList = shards[shardKey]
                    if(saddr not in shardList):
                        for replica in shardList:

                            r = requests.get('http://'+ replica + "/update-kv-store", timeout=1)
                            response_json = r.json()
                            #print(type(response_json))
                            for key in response_json:
                                kvstore[key] = response_json[key]

                print("Kvstore (after)", kvstore)
                
                ########################################################################
                
                ########################################################################

                # empty shard list
                print("Emptying shard list")
                
                for key in range( 1, (max(len(shards.keys()), new_shard_count)+1) ):
                    shards[key] = []

                # Initialize shards list
                # first check if the number of shards specified will work.

                for key in vc.keys():
                    print("VC at ", str(key), "set to 0")
                    vc[str(key)] = 0
                
                shardCount = new_shard_count
                print("enough in view to split into shards")

                print("shards before:", shards)
                print("shardcount is ", shardCount)

                #####################################################################
                num_nodes_in_shard = len(views_list) // int(new_shard_count)
                num_nodes_so_far = 0
                shardidx = 1
                #sort nodes into shards
                for view in views_list:
                    if(shardidx <= new_shard_count):
                        if (view == saddr): 
                            shardID = shardidx
                        if num_nodes_so_far < num_nodes_in_shard:
                            shards[shardidx].append(view)
                            num_nodes_so_far += 1
                        else:
                            shardidx += 1
                            if(shardidx <= new_shard_count):
                                num_nodes_so_far = 0
                                shards[shardidx].append(view)
                                num_nodes_so_far += 1
                                if (view == saddr): 
                                    shardID = shardidx
                # if uneven # of nodes, add an extra node to the last shard
                if( (len(views_list) % int(new_shard_count)) == 1):
                    shards[shardidx-1].append(views_list[-1])
                #####################################################################
                print("shards after:", shards)

                
            elif new_shard_count == -1:
                print("no new shardCount specified")
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'message' : "No shard-count specified in json"}), 'utf-8')
                self.wfile.write(response)
                return
            else:
                print("bad number provided: \nNot enough nodes to have redundancy in shards. Error 400")
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'message' : "Not enough nodes to provide fault-tolerance with the given shard count!"}), 'utf-8')
                self.wfile.write(response)
                return

            # Replace key/values for all servers
            for shardKey in shards.keys():
                kvstore_temp = {}
                for key in kvstore.keys():
                    if(magicHash(key, shardCount) == shardKey):
                        #add key to kvstore_temp if it matches the hash
                        kvstore_temp[key] = kvstore[key]
                for updateShard in shards[shardKey]:
                    if(str(saddr) != str(updateShard)):
                        print("saddr is", saddr)
                        print("update shard is", updateShard)
                        try:
                            print("Updating shards for replica", updateShard)
                            r2 = requests.put('http://'+ updateShard + "/broadcast-reshard-shards-put", timeout=1, json=shards)
                            print("Updating kvstore for replica", updateShard)
                            r = requests.put('http://'+ updateShard + "/broadcast-reshard-kvstore-put", timeout=1, json=kvstore_temp)
                        except:
                            print("issue with the broadcasted update to shards or kvstore")
                            #broadcast delete view?
            
            # Set the proper kvstore values for kvstore on the current saddr
            kvstore_temp = {}
            for key in kvstore.keys():
                if(magicHash(key, shardCount) == shardID):
                    #add key to kvstore_temp if it matches the hash
                    kvstore_temp[key] = kvstore[key]
            kvstore.clear()
            for key in kvstore_temp:
                kvstore[key] = kvstore_temp[key]

            #send response                
            self._set_headers(response_code=200)
            response = bytes(json.dumps({"message": "Resharding done successfully"}), 'utf-8')
            self.wfile.write(response)
            

        elif "/broadcast-reshard-kvstore-put" in str(self.path):
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            print("the new kvstore is", data)
            kvstore.clear()
            for key in data.keys():
                kvstore[key] = data[key]

            self._set_headers(response_code=200)
            response = bytes(json.dumps({"message": "updated kvstore correctly"}), 'utf-8')
            self.wfile.write(response)
        
        elif "/broadcast-reshard-shards-put" in str(self.path):
            # global shardID
            # global shardCount
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            print("the new shards is", data)
            shards.clear()
            for key in data.keys():
                shards[int(key)] = data[key]
            
            for key in shards.keys():
                if(saddr in shards[key]):
                    print("This node is being assigned to shard: ", key)
                    shardID = int(key)
            
            print("broadcast view shardcount (before)", shardCount)
            #global shardCount
            shardCount = len(shards.keys())
            print("broadcast view shardcount (after)", shardCount)
            
            for key in vc.keys():
                print("VC at ", str(key), "set to 0")
                vc[str(key)] = 0

            self._set_headers(response_code=200)
            response = bytes(json.dumps({"message": "updated shards correctly"}), 'utf-8')
            self.wfile.write(response)
            

        elif "/key-value-store-shard/add-member/" in str(self.path):
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            new_string = self.data_string.decode()
            new_string = new_string.replace('{', '')
            new_string = new_string.replace('}', '')
            new_string = new_string.replace('"', '')
            new_instance = new_string.split(": ")[1]
            shardID_str = str(self.path).split("/add-member/",1)[1]

            print("new instance: ", str(new_instance), "shard: ", shardID_str)

            for x in views_list:
                if (x != saddr):
                    print("http://" + str(x) + "/broadcast-shard-put")
                    # r = requests.put('http://' + x + "/broadcast-shard-put", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : new_instance, "shard_id": shardID_str})
                    try:
                        print("TRY: broadcasting SHARD PUT value to ", x)
                        r = requests.put('http://' + x + "/broadcast-shard-put", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : new_instance, "shard_id": shardID_str})
                    except:
                        print("EXCEPT: broadcasting DELETE view ", x)
                        views_list.remove(x)
                        for y in views_list:
                            print("Broadcasting DELETE downed instance ", x, "to ", y)
                            if (y != saddr) and (y != x):
                                try:
                                    r = requests.delete('http://' + y + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : x})
                                except:
                                    print("broadcast instance is down or busy")


            # append new instance to local shard
            print("Shard(before)", shards)
            if new_instance not in shards[int(shardID_str)]:
                shards[int(shardID_str)].append(new_instance)
            print("Shard(after)", shards)

            self._set_headers(response_code=200)
            response = bytes(json.dumps({'bogus' : "pp"}), 'utf-8')
            self.wfile.write(response)

        elif "/broadcast-shard-put" in str(self.path):
            # global shardID
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            shardID_str = data["shard_id"]
            new_instance = data["socket-address"]
            try:
                new_instance = new_instance.strip()
                new_instance = new_instance.replace('{', '')
                new_instance = new_instance.replace('}', '')
                new_instance = new_instance.replace('"', '')
                new_instance = new_instance.replace(' ', '')
            except:
                print("error in stripping")
            
            print("new_instance:", str(new_instance))
            print("saddr:       ", str(saddr))
            if (str(new_instance) == str(saddr)):
                #global shardID
                shardID = int(shardID_str)
                print("shardID updated, update kv and vc from others in the shard.")
                print("Shards (BEFORE) is", shards)
                #######################################################################################
                # GET SHARDS
                print(type(self.client_address))
                print(str(self.client_address[0]) + ":8085")
                for replica in views_list:
                    if (replica != saddr) and (replica != (str(self.client_address[0]) + ":8085")):
                        print("requesting http://" + replica + "/update-shard-dict")
                        try:
                            temp = "http://"+ str(replica) + "/update-shard-dict"
                            r = requests.get(temp, timeout=1)
                            response_json = r.json()
                            print(type(response_json))
                            for key in response_json:
                                shards[int(key)] = response_json[key]
                            break
                        except Exception as e: 
                            print("Exception was: ")
                            print(e)
                            print("replica is not up yet")
                    
                print("Shards (AFTER) is", shards)
                # GET the kvstore and vc from other nodes in the shard.
                for replica in views_list:
                    if ( (replica != saddr) and (replica in shards[shardID]) and (replica != (str(self.client_address[0]) + ":8085")) ):
                        print("requesting http://" + replica + "/update-kv-store")
                        try:
                            r = requests.get('http://'+ replica + "/update-kv-store", timeout=1)
                            response_json = r.json()
                            print(type(response_json))
                            for key in response_json:
                                kvstore[key] = response_json[key]
                            break
                        except:
                            print("replica is not up yet")

                for replica in views_list:
                    vc[str(replica)] = 0
                    if ( (replica != saddr) and (replica in shards[shardID]) and (replica != (str(self.client_address[0]) + ":8085")) ):
                        print("requesting http://" + replica + "/update-vc-store")
                        try:
                            r = requests.get('http://'+ replica + "/update-vc-store", timeout=.5)
                            response_json = r.json()
                            print(type(response_json))
                            for key in response_json:
                                vc[str(key)] = max(vc[str(key)],response_json[key])
                            break
                        except:
                            print("replica is not up yet")
                #######################################################################################

            print("Shard(before)", shards); 
            if (new_instance not in shards[int(shardID_str)]) and (shardID != -1):
                shards[int(shardID_str)].append(new_instance)
            print("Shard(after)", shards)

            self._set_headers(response_code=200)
            response = bytes(json.dumps({'bogus' : "pp"}), 'utf-8')
            self.wfile.write(response)

        # VIEW operations
        elif "/broadcast-view-put" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            new_string = self.data_string.decode()
            new_string = new_string.replace('{', '')
            new_string = new_string.replace('}', '')
            new_string = new_string.replace('"', '')
            new_string = new_string.split(": ")[1]
            
            if new_string in views_list:
                print("    view already in views_list")
                #print("    instance should already be in vc")
                #print(vc)
                self._set_headers(response_code=404)
                response = bytes(json.dumps({"bogus" : "doesnt matter", "message" : "done", "causal-metadata": "test" }), 'utf-8')
                self.wfile.write(response)
                return
            else:
                print("    View was not already in views_list, adding it now.")
                #print("    vc before adding it")
                #print(vc)
                vc[str(new_string)] = 0
                views_list.append(new_string)
                self._set_headers(response_code=200)
                response = bytes(json.dumps({"yee" : "fasholly", "message" : "we lit", "causal-metadata": "test" }), 'utf-8')
                self.wfile.write(response)
            #print("    vc after adding it")
            #print(vc)
            print("    View after PUT: ")
            print(views_list)
            return
        
        elif "/key-value-store-view" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            new_string = self.data_string.decode()
            new_string = new_string.replace('{', '')
            new_string = new_string.replace('}', '')
            new_instance = new_string.split(": ")[1]
            print("    new instance to add into view: " + str(new_instance))

            if new_instance not in views_list:
                for x in views_list:
                    if (x != saddr):
                        try:
                            print("    TRY: broadcasting PUT value ", new_instance, "to ", x)
                            r = requests.put('http://' + x + "/broadcast-view-put", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : new_instance})
                        except:
                            print("    EXCEPT: broadcasting DELETE view ", x)
                            views_list.remove(x)
                            for y in views_list:
                                print("    Broadcasting DELETE downed instance ", x, "to ", y)
                                if (y != saddr) and (y != x):
                                    try:
                                        r = requests.delete('http://' + y + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : x})
                                    except:
                                        print("    broadcast instance is down or busy")

                views_list.append(new_instance)
                #print("Views List (after PUT)", views_list)
                self._set_headers(response_code=201) 
                response = bytes(json.dumps({'message' : "Replica added successfully to the view", "causal-metadata":"test"}), 'utf-8')
                self.wfile.write(response)
            else:
                self._set_headers(response_code=404)
                response = bytes(json.dumps({'error' : "Socket address already exists in the view", "message" : "Error in PUT", "causal-metadata":"test"}), 'utf-8')
                self.wfile.write(response)
      
        elif "/broadcast-key-put/" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            keystr = str(self.path).split("/broadcast-key-put/",1)[1]
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            try:
                vc_temp = json.loads(data["causal-metadata"])
            except:
                vc_temp = ""
            for x in vc_temp:
                vc[str(x)] = vc_temp[str(x)]
            vc_str = json.dumps(vc_temp)

            if(len(keystr) > 0 and len(keystr) < 50):
                if "value" not in data:
                    self._set_headers(response_code=400)
                    response = bytes(json.dumps({'error' : "Value is missing", 'message' : "Error in PUT", "causal-metadata":vc_str}), 'utf-8')
                    self.wfile.write(response)
                elif keystr in kvstore:
                    kvstore[keystr] = data["value"]
                    self._set_headers(response_code=200)
                    response = bytes(json.dumps({'message' : "Updated successfully", 'replaced' : 'True', "causal-metadata":vc_str}), 'utf-8')
                    self.wfile.write(response)
                else:
                    kvstore[keystr] = data["value"]
                    self._set_headers(response_code=201)
                    response = bytes(json.dumps({'message' : "Added successfully", 'replaced' : 'False' , "causal-metadata":vc_str}), 'utf-8')
                    self.wfile.write(response)
            elif (len(keystr) > 50):
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'error' : "Key is too long", 'message' : "Error in PUT", "causal-metadata":vc_str}), 'utf-8')
                self.wfile.write(response)
            

        else:
            if "/key-value-store/" in str(self.path):
                # global shardID
                self.data_string = self.rfile.read(int(self.headers['Content-Length']))
                print(self.data_string)
                data = json.loads(self.data_string)
                try:
                    vc_temp = json.loads(data["causal-metadata"])
                except:
                    vc_temp = "" 
                vc_str = json.dumps(vc_temp)

                keystr = str(self.path).split("/key-value-store/",1)[1]

                insertShard = magicHash(keystr, shardCount)
                # if this key belongs in a different shard
                if(shardID != insertShard):
                    print("This shard does not have this key. forwarding to a node in the correct shard")
                    # grab last server in shard insertShard
                    
                    # forward request to proper shard
                    inserted = False
                    index = 0
                    while( (inserted == False) and (index < len(shards[insertShard])) ):
                        node = (shards[insertShard])[index]
                        if(node != saddr):
                            try:
                                print("    Trying: broadcast the PUT to the correct shard at ", node)
                                r = requests.put('http://' + node + "/key-value-store/" + keystr, timeout=5, allow_redirects=True, json=data)
                                inserted = True
                                #forward response from other node to client
                                self._set_headers(r.status_code)
                                self.wfile.write(r.content)

                            except:
                                x = node
                                print("    EXCEPT: broadcasting DELETE view ", x)
                                if(x in views_list):
                                    views_list.remove(x)
                                else:
                                    print("lol")
                                for y in views_list:
                                    print("    Broadcasting DELETE downed instance ", x, "to ", y)
                                    if (y != saddr) and (y != x):
                                        try:
                                            r = requests.delete('http://' + y + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : x})
                                        except:
                                            print("    broadcast instance is down or busy")
                        index += 1
                    #Failed insert
                    if(inserted == False):
                        self._set_headers(response_code=500)
                        response = bytes(json.dumps({'error' : "Shard is down", 'message' : "Error in PUT", "causal-metadata":vc_str}), 'utf-8')
                        self.wfile.write(response)

                # if the key belongs in THIS shard
                else:
                    # check vector clock stuff against all other servers in shard (for causal consistency)
                    if vc_temp != "":
                        for x in vc_temp:
                            print("vc_temp[",x ,"] is ", str(vc_temp[str(x)]))
                            print("VC[",x,"] ", " is ", str(vc[str(x)]))
                            if vc_temp[x] > vc[x]:
                                print("element is bigger kekw")
                                for replica in views_list:
                                    if(replica in list(shards[shardID]) and (replica != saddr)):
                                        try:
                                            r = requests.get('http://'+ replica + "/update-kv-store", timeout=1)
                                            response_json = r.json()
                                            print(type(response_json))
                                            for key in response_json:
                                                kvstore[key] = response_json[key]

                                            r = requests.get('http://'+ replica + "/update-vc-store", timeout=1)
                                            response_json = r.json()
                                            print(type(response_json))
                                            for key in response_json:
                                                vc[key] = max(vc[key],response_json[key])
                                        except:
                                            print("we have failed")
                                        try:
                                            r = requests.put('http://' + replica + "/broadcast-view-put", timeout=1, allow_redirects=False, json={"socket-address" : saddr})
                                        except:
                                            print("replica ", replica, " in view is not yet live.")
                                        break

                    # do the PUT on the current node and related shards 
                    # TODO: distribute PUT to only related shards, not all
                    if(len(keystr) > 0 and len(keystr) < 50):
                        try:
                            vc_temp = json.loads(data["causal-metadata"])
                        except:
                            vc_temp = ""
                        print("keyvaluestore vc_temp ")
                        print(vc_temp)
                        if "value" not in data:
                            self._set_headers(response_code=400)
                            response = bytes(json.dumps({'error' : "Value is missing", 'message' : "Error in PUT", "causal-metadata": vc_str}), 'utf-8')
                            self.wfile.write(response)
                        elif keystr in kvstore:
                            kvstore[keystr] = data["value"]
                            # INCREMENT VECTOR CLOCK
                            vc[str(saddr)] = vc[str(saddr)] + 1
                            vc_str = json.dumps(vc)

                            # send PUT req to all other views in the same shard
                            self.distributePUT(keystr, shardID, vc_str , data)
                            
                            self._set_headers(response_code=200)
                            response = bytes(json.dumps({'message' : "Updated successfully", 'replaced' :True, "causal-metadata": vc_str, "shard-id": shardID}), 'utf-8')
                            self.wfile.write(response)
                            return
                        else:
                            kvstore[keystr] = data["value"]
                            # INCREMENT VECTOR CLOCK
                            if(str(saddr) not in vc):
                                vc[str(saddr)] = 1
                            else:
                                vc[str(saddr)] = vc[str(saddr)] + 1
                            vc_str = json.dumps(vc)

                            # send PUT req to all other views in the same shard
                            self.distributePUT(keystr, shardID, vc_str, data)

                            self._set_headers(response_code=201)
                            response = bytes(json.dumps({'message' : "Added successfully", 'replaced' :False, "causal-metadata":vc_str, "shard-id": shardID}), 'utf-8')
                            self.wfile.write(response)
                            return
                    elif (len(keystr) > 50):
                        self._set_headers(response_code=400)
                        response = bytes(json.dumps({'error' : "Key is too long", 'message' : "Error in PUT", "causal-metadata":vc_str}), 'utf-8')
                        self.wfile.write(response)
                    
            else:
                self._set_headers(response_code=500)
        
        return
    
    # if the server recieves a DELETE request, it is directed here
    def do_DELETE(self):
        global shardID
        global shardCount
        print("\n[+] recieved DELETE request from: " + str(self.client_address[0]) + " to path: " + str(self.path) + "\n")
        view_list_str = []
        for x in views_list:
            view_list_str.append(str(x))

        if "/broadcast-view-delete" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            new_string = self.data_string.decode()
            new_string = new_string.replace('{', '')
            new_string = new_string.replace('}', '')
            new_string = new_string.replace('"', '')
            delete_replica = new_string.split(": ")[1]
            if delete_replica.strip() not in view_list_str:
                print("    view not in views_list")
                self._set_headers(response_code=200)
                response = bytes(json.dumps({"bogus" : "doesnt matter", "message" : "done", "causal-metadata":"test"}), 'utf-8')
                self.wfile.write(response)
                return
            try:
                views_list.remove(delete_replica)
            except:
                print("already deleted")
            self._set_headers(response_code=200)
            response = bytes(json.dumps({"bogus" : "doesnt matter", "message" : "done", "causal-metadata":"test"}), 'utf-8')
            self.wfile.write(response)

        elif "/broadcast-key-delete" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            keystr = str(self.path).split("/broadcast-key-delete/",1)[1]
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            try:
                vc_temp = json.loads(data["causal-metadata"])
            except:
                vc_temp = ""
            for x in vc_temp:
                vc[str(x)] = vc_temp[str(x)]
            vc_str = json.dumps(vc_temp)

            if(len(keystr) > 0 and len(keystr) < 50):
                if keystr in kvstore:
                    del kvstore[keystr]
                    self._set_headers(response_code=200)
                    response = bytes(json.dumps({"message" : "Deleted successfully", "causal-metadata":vc_str}), 'utf-8')
                else:
                    self._set_headers(response_code=404)
                    response = bytes(json.dumps({"doesExist" : False, "error" : "Key does not exist", "message" : "Error in DELETE", "causal-metadata":vc_str}), 'utf-8')
            elif (len(keystr) > 50):
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'error' : "Key is too long", 'message' : "Error in DELETE", "causal-metadata":vc_str}), 'utf-8')
            elif(len(keystr) == 0):
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'error' : "Key not specified", 'message' : "Error in DELETE", "causal-metadata":vc_str}), 'utf-8')
            self.wfile.write(response)

        elif "/key-value-store-view" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            new_string = self.data_string.decode()
            new_string = new_string.replace('{', '')
            new_string = new_string.replace('}', '')
            delete_replica = new_string.split(": ")[1]
            print(delete_replica)
            if delete_replica not in views_list:
                self._set_headers(response_code=404)
                response = bytes(json.dumps({"error" : "Socket address does not exist in the view", "message" : "Error in DELETE", "causal-metadata":"test"}), 'utf-8')
                self.wfile.write(response)
                return
            views_list.remove(delete_replica)
            #send delete to all other replicas in the view lsit
            for x in views_list:
                if x != saddr:
                    try:
                        print( "    TRY: deleting ", str(delete_replica), " at ", str(x) )
                        r = requests.delete('http://' + x + "/broadcast-view-delete", allow_redirects=False, headers=self.headers, json={"socket-address" : delete_replica})
                    except:
                        for y in views_list:
                            print( "    EXCEPT: broadcasting delete of ", str(x), " at ", str(y) )  
                            if (self.client_address[0] + ":8085" != y) and (x != y):
                                try:
                                    r = requests.delete('http://' + y + "/broadcast-view-delete" , allow_redirects = False, headers=self.headers, json={"socket-address" : delete_replica})
                                except:
                                    print("    instance is also down or busy")
                else:
                    print("    Cannot send request to self")
                #print(views_list)
                
            self._set_headers(response_code=200)
            self.end_headers()
            response = bytes(json.dumps({'message' : "Replica deleted successfully from the view", "causal-metadata":"test"}), 'utf-8')
            self.wfile.write(response)

        elif "/key-value-store/" in str(self.path):
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            keystr = str(self.path).split("/key-value-store/",1)[1]

            insertShard = magicHash(keystr, shardCount)
            # if this key belongs in a different shard
            if(shardID != insertShard):
                print("This shard does not have this key. forwarding to a node in the correct shard")
                # grab last server in shard insertShard
                
                # forward request to proper shard
                inserted = False
                index = 0
                while( (inserted == False) and (index < len(shards[insertShard])) ):
                    node = (shards[insertShard])[index]
                    try:
                        print("    Trying: broadcast the DELETE to the correct shard at ", node)
                        r = requests.delete('http://' + node + "/key-value-store/" + keystr, timeout=5, allow_redirects=True, json=data)
                        inserted = True
                        #forward response from other node to client
                        self._set_headers(r.status_code)
                        self.wfile.write(r.content)

                    except:
                        x = node
                        print("    EXCEPT: broadcasting DELETE view ", x)
                        views_list.remove(x)
                        for y in views_list:
                            print("    Broadcasting DELETE downed instance ", x, "to ", y)
                            if (y != saddr) and (y != x):
                                try:
                                    r = requests.delete('http://' + y + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : x})
                                except:
                                    print("    broadcast instance is down or busy")
                    index += 1
                #Failed DELETE
                if(inserted == False):
                    self._set_headers(response_code=500)
                    response = bytes(json.dumps({'error' : "Shard is down"}), 'utf-8')
                    self.wfile.write(response)

            # if the key belongs in THIS shard
            # do the DELETE on the current node and related shards 
            elif (shardID == insertShard) and (len(keystr) > 0 and len(keystr) < 50):
                print("SHARD ID MATCH!")
                print("shard id is ", shardID)
                print("insertShard is ", insertShard)
                try:
                    vc_temp = json.loads(data["causal-metadata"])
                except:
                    vc_temp = ""
                for x in vc_temp:
                    print("vc_temp[",x ,"] is ", str(vc_temp[str(x)]))
                    print("VC[",x,"] ", " is ", str(vc[str(x)]))
                    if vc_temp[str(x)] > vc[str(x)]:
                        for replica in views_list:
                            if(replica in list(shards[shardID]) and (replica != saddr)):
                                try:
                                    r = requests.get('http://'+ replica + "/update-kv-store", timeout=1)
                                    response_json = r.json()
                                    print(type(response_json))
                                    for key in response_json:
                                        kvstore[key] = response_json[key]

                                    r = requests.get('http://'+ replica + "/update-vc-store", timeout=1)
                                    response_json = r.json()
                                    print(type(response_json))
                                    for key in response_json:
                                        vc[str(key)] = max(vc[str(key)],response_json[key])
                                except:
                                    print("we have failed")
                                try:
                                    r = requests.put('http://' + replica + "/broadcast-view-put", timeout=1, allow_redirects=False, json={"socket-address" : saddr})
                                except:
                                    print("replica ", replica, " in view is not yet live.")
                                break

                if(len(keystr) > 0 and len(keystr) < 50):
                    if keystr in kvstore:
                        # INCREMENT VECTOR CLOCK
                        vc[str(saddr)] = vc[str(saddr)] + 1
                        vc_str = json.dumps(vc) 
                        # Send key DELETE to all other replicas
                        for replica in views_list:
                            if(replica in list(shards[shardID]) and (replica != saddr)):
                                try:
                                    print("    Broadcasting DELETE key value ", str(keystr), " to ", str(replica))
                                    r = requests.delete('http://' + replica + "/broadcast-key-delete/" + keystr, allow_redirects=False, headers=self.headers, json={"causal-metadata": vc_str})
                                except:
                                    print("    The instance is down, broadcasting delete view to all up instances")
                                    views_list.remove(replica)
                                    for y in views_list:
                                        print("    Broadcasting DELETE downed instance ", replica, "to ", y)
                                        if (y != saddr) and (y != replica):
                                            try:
                                                r = requests.delete('http://' + y + "/broadcast-view-delete", allow_redirects=False, headers=self.headers, json={"socket-address" : replica})
                                            except:
                                                print("    instance is also down or busy")
                        del kvstore[keystr]
                        self._set_headers(response_code=200)
                        response = bytes(json.dumps({"message" : "Deleted successfully", "causal-metadata":vc_str}), 'utf-8')
                        self.wfile.write(response)
                        
                    else:
                        vc_str = json.dumps(vc) 
                        self._set_headers(response_code=404)
                        response = bytes(json.dumps({"doesExist" : False, "error" : "Key does not exist", "message" : "Error in DELETE", "causal-metadata":vc_str}), 'utf-8')
                        self.wfile.write(response)

            elif (len(keystr) > 50):
                vc_str = json.dumps(vc)
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'error' : "Key is too long", 'message' : "Error in DELETE", "causal-metadata":vc_str}), 'utf-8')
                self.wfile.write(response)
            elif(len(keystr) == 0):
                vc_str = json.dumps(vc)
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'error' : "Key not specified", 'message' : "Error in DELETE", "causal-metadata":vc_str}), 'utf-8')
                self.wfile.write(response)
         
        else:
            #default 500 code to clean up loose ends
            self._set_headers(response_code=500)
            response = bytes(json.dumps({"Error in DELETE": "Unknown Error"}), 'utf-8')
            self.wfile.write(response)
        
        return

    

def run(server_class=http.server.HTTPServer, handler_class=requestHandler, addr='0.0.0.0', port=8085):
    # this function initializes and runs the server on the class defined above
    server_address = (addr, port)
    httpd = server_class(server_address, handler_class)

    for replica in views_list:
        if (replica != saddr):
            try:
                r = requests.put('http://' + replica + "/broadcast-view-put", timeout=.5, allow_redirects=False, json={"socket-address" : saddr})
            except:
                print("replica ", replica, " in view is not yet live.")
    if(shardID != -1):
        for replica in views_list:
            if (replica != saddr) and (replica in list(shards[shardID])):
                print("requesting http://" + replica + "/update-kv-store")
                try:
                    r = requests.get('http://'+ replica + "/update-kv-store", timeout=.5)
                    response_json = r.json()
                    print(type(response_json))
                    for key in response_json:
                        kvstore[key] = response_json[key]
                    break
                except:
                    print("replica is not up yet")

        for replica in views_list:
            vc[str(replica)] = 0
            if (replica != saddr) and (replica in list(shards[shardID])):
                print("requesting http://" + replica + "/update-vc-store")
                try:
                    r = requests.get('http://'+ replica + "/update-vc-store", timeout=.5)
                    response_json = r.json()
                    print(type(response_json))
                    for key in response_json:
                        vc[str(key)] = max(vc[str(key)],response_json[key])
                    break
                except:
                    print("replica is not up yet")
            print("Vector clock of ", replica, " is ", vc[str(replica)])
    else:
        print("shardID uninitialized, will update kvstore later.")

    print(f"Starting HTTP server on {addr}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()


if __name__ == '__main__':
    d_ip = ''
    d_port = 8085
    try:
        saddr = os.environ['SOCKET_ADDRESS']
        if len(saddr) > 0:
            print("SOCKET_ADDRESS: " + str(saddr))
        views = os.environ['VIEW']
        views_list = views.split(",")
        if len(saddr) > 0:
            print("VIEWS: " + str(views))
        shardCount = os.environ['SHARD_COUNT']
        shardCount = int(shardCount)
        print("Shard Count: ", str(shardCount))

    except:
        print("main instance")
        main_flag = True

    # Initialize empty list (of nodes) for each shard
    for shard in range(1, int(shardCount)+1):
           shards[shard] = []

    # Initialize shards list
    if len(views_list) / int(shardCount) >= 2:
        print("enough in view to split into shards")
        #TODO split views into shards and store in shards dict.
        num_nodes_in_shard = len(views_list) // int(shardCount)
        num_nodes_so_far = 0
        shardidx = 1

        #sort nodes into shards
        for view in views_list:
            if(shardidx <= shardCount):
                if (view == saddr): 
                    #print("view" , view, " and saddr ", saddr, "match")
                    shardID = shardidx
                    # print("shard ID is: ", shardID)
                if num_nodes_so_far < num_nodes_in_shard:
                    shards[shardidx].append(view)
                    num_nodes_so_far += 1
                    #print("num nodes so far", num_nodes_so_far, " in shard", shardidx)
                    #print("shard current status: ", shards)
                else:
                    #print("shard", shardidx, " is full, go next")
                    shardidx += 1
                    if(shardidx <= shardCount):
                        num_nodes_so_far = 0
                        shards[shardidx].append(view)
                        num_nodes_so_far += 1
                        if (view == saddr): 
                            #print("view" , view, " and saddr ", saddr, "match")
                            shardID = shardidx
                            # print("shard ID is: ", shardID)
            
        # if uneven # of nodes, add an extra node to the last shard
        if( (len(views_list) % int(shardCount)) == 1):
            shards[shardidx-1].append(views_list[-1])
        print(shards)
    elif(shardID == -1):
        print("shardCount not specified.")
    else:
        print("not enough nodes to have redundancy in shards. exiting program now")
        exit(0)
    
    print("shard ID is: ", shardID)
    print(main_flag)
    x = 0
    for arg in argv:
        print("arg" + str(x) + ": " + str(argv[x]))
        x = x+1

    if len(argv) == 2:
        #call the run function with custom port
        run(port=int(argv[1]))
    else:
        run()