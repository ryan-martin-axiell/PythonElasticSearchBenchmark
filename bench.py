from elasticsearch import Elasticsearch
import uuid
import time
import sys

if (len(sys.argv) < 2):
	print("Please supply a table name")
	sys.exit(1)

table = sys.argv[1]

fileuuid = table + "." + str(uuid.uuid4())
fileuuid = "/dev/null"

es = Elasticsearch()

base = {
	"_source": "id",
	"sort": [
		{ "id.keyword": "asc" }
	],
	"size": 5000
}

start = time.time()

with open(fileuuid, 'w') as f:
	res = es.search(index=table, doc_type="_doc", body=base)
	count = len(res['hits']['hits'])
	size = res['hits']['total']
	last = res['hits']['hits'][-1]

	f.write(str(res['hits']['hits']))

	while(count <= size):
		base['search_after'] = last['sort']
		res = es.search(index=table, doc_type="_doc", body=base)
		hits = res['hits']['hits']

		f.write(str(hits))

		count += len(hits)
		if (len(hits) == 0):
			break

		last = hits[-1]
		print("{}/{} last id: {}".format(count,size,last['_source']['id']))

end = time.time()
print(str(end - start) + "s")
