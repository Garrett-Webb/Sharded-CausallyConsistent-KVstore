# server.py

# Assignment 2
# CruzIDs
# Garrett Webb: gswebb
# Kai Hsieh: kahsieh
# Rahul Arora: raarora

#CSE138 Kuper Spring 2021

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

class requestHandler(http.server.BaseHTTPRequestHandler):
    def _set_headers(self, response_code):
        self.send_response(response_code)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def do_GET(self):
        print("\n[+] recieved GET request from: " + str(self.client_address[0]) + " to path: " + str(self.path) + "\n")
        if "/update-vc-store" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            print("vc to send back:")
            print(vc)
            self._set_headers(response_code=200)
            response = bytes(json.dumps(vc), 'utf-8')
            self.wfile.write(response)

        elif "/checkview/" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self._set_headers(response_code=200)
            response = bytes(json.dumps(vc), 'utf-8')
            self.wfile.write(response)

        elif "/key-value-store-view" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            down_instances = []
            for x in views_list:
                try:
                    if x != saddr:
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
                            print("instance ", + y + " is either down or busy")
            
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
            try:
                vc_temp = json.loads(data["causal-metadata"])
            except:
                vc_temp = ""
            for x in vc_temp:
                print("vc_temp[",x ,"] is ", str(vc_temp[x]))
                print("VC[",x,"] ", " is ", str(vc[x]))
                if vc_temp[x] > vc[x]:
                    print("element is bigger kekw")
                    for replica in views_list:
                        if replica != saddr:
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
            
            print("BROADCAST PUT causal metadata")
            print(vc)
            keystr = str(self.path).split("/key-value-store/",1)[1]
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

    def do_PUT(self):
        print("\n[+] recieved PUT request from: " + str(self.client_address[0]) + " to path: " + str(self.path) + "\n")

        if "/broadcast-view-put" in str(self.path): # and any(self.client_address[0] in string for string in views_list):
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            new_string = self.data_string.decode()
            new_string = new_string.replace('{', '')
            new_string = new_string.replace('}', '')
            new_string = new_string.replace('"', '')
            new_string = new_string.split(": ")[1]
            
            if new_string in views_list:
                print("    view already in views_list")
                print("    instance should already be in vc")
                print(vc)
                self._set_headers(response_code=404)
                response = bytes(json.dumps({"bogus" : "doesnt matter", "message" : "done", "causal-metadata": "test" }), 'utf-8')
                self.wfile.write(response)
                return
            else:
                print("    vc before adding it")
                print(vc)
                vc[new_string] = 0
                views_list.append(new_string)
                self._set_headers(response_code=200)
                response = bytes(json.dumps({"yee" : "fasholly", "message" : "we lit", "causal-metadata": "test" }), 'utf-8')
                self.wfile.write(response)
            print("    vc after adding it")
            print(vc)
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
                vc[x] = vc_temp[x]
            vc_str = json.dumps(vc_temp)

            if(len(keystr) > 0 and len(keystr) < 50):
                if "value" not in data:
                    self._set_headers(response_code=400)
                    response = bytes(json.dumps({'error' : "Value is missing", 'message' : "Error in PUT", "causal-metadata":vc_str}), 'utf-8')
                elif keystr in kvstore:
                    kvstore[keystr] = data["value"]
                    self._set_headers(response_code=200)
                    response = bytes(json.dumps({'message' : "Updated successfully", 'replaced' :True, "causal-metadata":vc_str}), 'utf-8')
                else:
                    kvstore[keystr] = data["value"]
                    self._set_headers(response_code=201)
                    response = bytes(json.dumps({'message' : "Added successfully", 'replaced' :False, "causal-metadata":vc_str}), 'utf-8')
            elif (len(keystr) > 50):
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'error' : "Key is too long", 'message' : "Error in PUT", "causal-metadata":vc_str}), 'utf-8')
                
            self.wfile.write(response)

        else:
            if "/key-value-store/" in str(self.path):
                self.data_string = self.rfile.read(int(self.headers['Content-Length']))
                data = json.loads(self.data_string)
                try:
                    vc_temp = json.loads(data["causal-metadata"])
                except:
                    vc_temp = "" 
                for x in vc_temp:
                    print("vc_temp[",x ,"] is ", str(vc_temp[x]))
                    print("VC[",x,"] ", " is ", str(vc[x]))
                    if vc_temp[x] > vc[x]:
                        print("element is bigger kekw")
                        for replica in views_list:
                            if replica != saddr:
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
                vc_str = json.dumps(vc_temp)

                keystr = str(self.path).split("/key-value-store/",1)[1]
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
                    elif keystr in kvstore:
                        kvstore[keystr] = data["value"]
                        # INCREMENT VECTOR CLOCK
                        vc[saddr] = vc[saddr] + 1
                        vc_str = json.dumps(vc)

                        # Send key PUT to all other replicas
                        for replica in views_list:
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
                                                r = requests.put('http://' + y + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : replica})
                                            except:
                                                print("    instance is also down or busy")
                        self._set_headers(response_code=200)
                        response = bytes(json.dumps({'message' : "Updated successfully", 'replaced' :True, "causal-metadata": vc_str}), 'utf-8')
                        self.wfile.write(response)
                        return
                    else:
                        kvstore[keystr] = data["value"]
                        # INCREMENT VECTOR CLOCK
                        vc[saddr] = vc[saddr] + 1
                        vc_str = json.dumps(vc)
                        # Send key PUT to all other replicas
                        for replica in views_list:
                            if (replica != saddr):
                                try:
                                    print("    Broadcasting PUT value ", str(keystr), " to ", str(replica))
                                    r = requests.put('http://' + replica + "/broadcast-key-put/" + keystr, timeout=1, allow_redirects=False, headers=self.headers, json={"value" : data["value"], "causal-metadata": vc_str})
                                except:
                                    print("    The instance is down, broadcasting delete view to all up instances")
                                    for y in views_list:
                                        print("    Broadcasting DELETE downed instance ", replica, "to ", y)
                                        if (y != saddr) and (y != replica):
                                            try:
                                                r = requests.put('http://' + y + "/broadcast-view-delete", timeout=1, allow_redirects=False, headers=self.headers, json={"socket-address" : replica})
                                            except:
                                                print("    instance is also down or busy")
                        self._set_headers(response_code=201)
                        response = bytes(json.dumps({'message' : "Added successfully", 'replaced' :False, "causal-metadata":vc_str}), 'utf-8')
                        self.wfile.write(response)
                        return
                elif (len(keystr) > 50):
                    self._set_headers(response_code=400)
                    response = bytes(json.dumps({'error' : "Key is too long", 'message' : "Error in PUT", "causal-metadata":vc_str}), 'utf-8')
                
                self.wfile.write(response)
            else:
                self._set_headers(response_code=500)
        
        return
    
    def do_DELETE(self):
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
            views_list.remove(delete_replica)
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
                vc[x] = vc_temp[x]
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

            try:
                vc_temp = json.loads(data["causal-metadata"])
            except:
                vc_temp = ""
            for x in vc_temp:
                print("vc_temp[",x ,"] is ", str(vc_temp[x]))
                print("VC[",x,"] ", " is ", str(vc[x]))
                if vc_temp[x] > vc[x]:
                    for replica in views_list:
                        if replica != saddr:
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

            if(len(keystr) > 0 and len(keystr) < 50):
                if keystr in kvstore:
                    # INCREMENT VECTOR CLOCK
                    vc[saddr] = vc[saddr] + 1
                    vc_str = json.dumps(vc) 
                    # Send key DELETE to all other replicas
                    for replica in views_list:
                        if (replica != saddr):
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
                    
                else:
                    vc_str = json.dumps(vc) 
                    self._set_headers(response_code=404)
                    response = bytes(json.dumps({"doesExist" : False, "error" : "Key does not exist", "message" : "Error in DELETE", "causal-metadata":vc_str}), 'utf-8')
            elif (len(keystr) > 50):
                vc_str = json.dumps(vc)
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'error' : "Key is too long", 'message' : "Error in DELETE", "causal-metadata":vc_str}), 'utf-8')
            elif(len(keystr) == 0):
                vc_str = json.dumps(vc)
                self._set_headers(response_code=400)
                response = bytes(json.dumps({'error' : "Key not specified", 'message' : "Error in DELETE", "causal-metadata":vc_str}), 'utf-8')
            self.wfile.write(response)
         
        else:
            #default 500 code to clean up loose ends
            self._set_headers(response_code=500)
        
        return

def run(server_class=http.server.HTTPServer, handler_class=requestHandler, addr='0.0.0.0', port=8085):
    # this function initializes and runs the server on the class defined above
    server_address = (addr, port)
    httpd = server_class(server_address, handler_class)

    for replica in views_list:
        if replica != saddr:
            try:
                r = requests.put('http://' + replica + "/broadcast-view-put", timeout=1, allow_redirects=False, json={"socket-address" : saddr})
            except:
                print("replica ", replica, " in view is not yet live.")
    
    for replica in views_list:
        if replica != saddr:
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

    for view in views_list:
        vc[view] = 0
        if replica != saddr:
            print("requesting http://" + replica + "/update-vc-store")
            try:
                r = requests.get('http://'+ replica + "/update-vc-store", timeout=1)
                response_json = r.json()
                print(type(response_json))
                for key in response_json:
                    vc[key] = max(vc[key],response_json[key])
                break
            except:
                print("replica is not up yet")
        print("Vector clock of ", view, " is ", vc[view])

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

    except:
        print("main instance")
        main_flag = True
        
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