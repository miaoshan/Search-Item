import json
from whoosh.index import create_in
from whoosh.fields import *
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from whoosh.qparser import QueryParser

def loadFileContentIntoList(fileName): 
    f = open(fileName, "r")
    lines = list(f)
    f.close()
    return lines

def loadFileContentIntoStr(fileName):
    return open(fileName, 'r').read()

def searchByFieldAndWrite(fieldName, searchWord, writer):
    with ix.searcher() as searcher:
        query = QueryParser(fieldName, ix.schema)
        results = searcher.search(query.parse(searchWord), limit=None)
        writer.write(str.encode("<h3>number of results for field '" + fieldName + "' matching '" + searchWord + "' : " + str(len(results)) + "<br></h3>"))            
        for r in results:
            writer.write(str.encode(r.highlights(fieldName) + "<br><hr>"))

print("start indexing")
schema = Schema(product_categories_mapped=NUMERIC(stored=True)
                ,product_id=ID(stored=True)
                ,url=ID(stored=True)
                ,gender=ID(stored=True)
                ,brand=TEXT(stored=True)
                ,product_description=TEXT(stored=True)
                ,product_imgs_src=IDLIST(stored=True)
                ,source=TEXT(stored=True)
                ,product_categories=IDLIST(stored=True)
                ,image_urls=IDLIST(stored=True)
                ,image_paths=IDLIST(stored=True)
                ,image_checksums=IDLIST(stored=True)
                ,price=TEXT(stored=True)
                ,product_title=TEXT(stored=True))
ix = create_in("indexdir", schema)
writer = ix.writer()

#example
# writer.add_document(product_categories_mapped=[123]
#                     ,product_id="ny221a00q-n11"
#                     ,url="https://www.zalando.co.uk/native-youth-jumpsuit-olive-ny221a00q-n11.html"
#                     ,gender="women"
#                     ,brand="Native Youth"
#                     ,product_description="category: heading_material,  [ 100% Lyocell,  Outer fabric material,  Machine wash at 40\\xb0C, do not tumble dry, Machine wash on gentle cycle,  Washing instructions], type: attributes_map, subheading: Outer fabric material: 100% Lyocellcategory: heading_details,  [ Henley,  Neckline,  Side pockets,  Pockets,  Plain,  Pattern,  NY221A00Q-N11,  config_sk], type: attributes_map, subheading: Neckline: Henleycategory: heading_measure_and_fitting,  [ Our model is 70.5 \" tall and is wearing size S,  u\"Our models height\",  7/8 length,  Length,  Sleeveless,  Sleeve length,  22.5 \" (Size S),  Inner leg length,  19.5 \" (Size S),  Back width], type: attributes_map, subheading: Our model\\s height: Our model is 70.5 \" tall and is wearing size S"
#                     ,image_urls=["https://mosaic01.ztat.net/vgs/media/pdp-gallery/NY/22/1A/00/QN/11/NY221A00Q-N11@14.jpg"]
#                     ,product_imgs_src=["https://mosaic01.ztat.net/vgs/media/pdp-gallery/NY/22/1A/00/QN/11/NY221A00Q-N11@14.jpg"]
#                     ,source="www.zalando.co.uk"
#                     ,product_categories=["womens-clothing-playsuits-jumpsuits"]
#                     ,image_paths=["full/37431c6247e4964a8ec7f1a4954be8595d34bb18.jpg"]
#                     ,image_checksums=["10f6a0dc3db6b15724cebe9b1cb244be"]
#                     ,price="84.99"
#                     ,product_title="ASTER  - Jumpsuit - olive")
for idx,eachLine in enumerate(loadFileContentIntoList("garment_items.jl"), start=1):
    print(idx)
    jsonDoc = json.loads(eachLine)
    writer.add_document(product_categories_mapped=jsonDoc["product_categories_mapped"]
        ,product_id=jsonDoc["product_id"]
        ,url=jsonDoc["url"]
        ,gender=jsonDoc["gender"]
        ,brand=jsonDoc["brand"]
        ,product_description=jsonDoc["product_description"]
        ,image_urls=jsonDoc["image_urls"]
        ,product_imgs_src=jsonDoc["product_imgs_src"]
        ,source=jsonDoc["source"]
        ,product_categories=jsonDoc["product_categories"]
        ,price=jsonDoc["price"]
        ,product_title=jsonDoc["product_title"]
    )
#note that Whoosh is very slow. so here will be a long wait
#https://stackoverflow.com/questions/54822978/python-whoosh-taking-too-long-to-index-a-large-file
print("now committing, this will take a while")
writer.commit()

print("start sample query")
with ix.searcher() as searcher:
    query = QueryParser("product_description", ix.schema).parse("Lyocell")
    results = searcher.search(query)
    print("result array size for searching Lyocell in product_description: " + str(len(results)))
    
class SearchHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(str.encode(loadFileContentIntoStr("index.htm")))    
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        response = BytesIO()
        
        searchWord = body.decode("utf-8").replace("search=","")
        self.wfile.write(str.encode("<a href='/'>Back to Search page</a><br>"))   
        searchByFieldAndWrite("product_description", searchWord, self.wfile)
        searchByFieldAndWrite("product_title", searchWord, self.wfile)
        searchByFieldAndWrite("price", searchWord, self.wfile)
        searchByFieldAndWrite("brand", searchWord, self.wfile)
        searchByFieldAndWrite("product_categories", searchWord, self.wfile)
    
httpd = HTTPServer(('localhost', 8000), SearchHTTPRequestHandler)
print("starting http server")
httpd.serve_forever()