import webhoseio, json, logging, pandas as pd

#create class for authentication
webhoseio.config(token="e10a7458-2a35-432e-b5f4-9549bf5faea8")
logging.info(msg="Authenticated using unique token")

query_params = {
    "q": "language:english site_type:news thread.country:GB site_category:financial_news site_category:business",
    "sort": "crawled"
}

results = webhoseio.query("filterWebContent", query_params)
requestsCount = len(results['posts'])
# print(len(results))
print(requestsCount)
print(results)
#df = pd.DataFrame(data=results)

while results['posts']:
    with open('webhose.json', 'a', encoding='utf8') as outfile:
        output = json.dumps(results,
                            indent=4, sort_keys=True,
                            separators=(',', ': '), ensure_ascii=False)

        outfile.write(output)
        results = webhoseio.get_next()
        for posts in results['posts']:
            requestsCount += 1
        if len(results['posts']) <= 0:
            print(requestsCount)
            logging.info("All results collected")
            break
