## whitebox section
==============

Requirements (python 2.7.x)
-------

Core program requirements:
```sh
sudo apt-get -y install python-pip
sudo pip install -r requirements.txt
```

Kafka / Zookeeper plugin - python requirements

```sh
sudo pip install -r plugins/kafkabot/requirements.txt
```

Using the main program
-------

```sh
python -B monitor.py --plugin <plugin_name> --postjson <post_url> --extra "extra_string"
```

The options are

    plugin_name                 => Directory name under plugins
    post_url                    => Endpoint to send results
    extra_string                => Args to be sent to the plugin


Endpoint json message
-------

```json

{
 "timestamp": 1450183624565,
 "data": {
           "source": "dummy",
           "metric": "dummy.health",
           "data": "{\"causes\": [\"you know what\", \"with a dummy script\", \"life is green\"]}",
           "value": "OK",
           "timestamp": 1450183624565
         }
}

value: OK, WARN or ERROR
causes: stringified array of details about the results


{
"timestamp": 1450183624565,
"data": {
          "source": "kafkabot",
          "metric": "kafkabot.health",
          "causes": "topic / partition inconsistency in zookeeper,
          "value": "OK",
          "timestamp": 1450183624565
        }
}

{
"timestamp": 1450183624565,
"data": {
          "source": "kafkabot",
          "metric": "kafkabot.health",
          "causes": "zookeeper node(s) unreachable (localhost:2182), producer / consumer failed (sent -1, rcv_ok -1, rcv_ko -1)",
          "value": "ERROR",
          "timestamp": 1450183624565
        }
}
```


Plugin Development
-------
A plugin must implement a TestbotPlugin class, will a runner method taking into parameter the extra string argument used later by arg parse. This method shall return MonitorResult structure used by monitor.py to build a json message to be sent to the endpoint.


Dummy plugin
-------
It is our hello world example.

```sh
python -B monitor.py --plugin dummy --postjson http://localhost:8000 --extra "--display --friendly"
```


kafkabot
-------
Kafka / zookeeper

```sh
python -B monitor.py --plugin kafkabot --postjson http://localhost:8000 --extra "--brokerlist localhost:9050"
python -B monitor.py --plugin kafkabot --postjson http://localhost:8000 --extra "--brokerlist 192.168.1.130:9050,192.168.1.131:9050"

```

The plugin options are

    brokerlist                  => broker list



Testing with a local endpoint
-------
For development, you can run a simple helloworld http server supporting POST request with the following python script:

```python
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import SocketServer
import json
import random

class S(BaseHTTPRequestHandler):
	def do_GET(self):
    	self.send_response(200)
    	self.send_header('Content-type', 'text/html')
    	self.end_headers()

	def do_HEAD(self):
   		self.send_response(200)
   		self.send_header('Content-type', 'text/html')
   		self.end_headers()

    def do_POST(self):
        self.data_string = self.rfile.read(int(self.headers['Content-Length']))
        data = json.loads(self.data_string)
        print json.dumps(data)
        self.send_response(201)
        self.end_headers()
        return


def run(server_class=HTTPServer, handler_class=S, port=8000):
	server_address = ('localhost', port)
	httpd = server_class(server_address, handler_class)
	print 'Starting httpd...'
	httpd.serve_forever()

if __name__ == "__main__":
	run()
```
