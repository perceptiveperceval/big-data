import requests
import os
import time
from vnstock import listing_companies
import threading
import json
from tqdm import tqdm


def get_size_dir(dir_path):
    total_size = 0
    for dirpath, _, filenames in os.walk(dir_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size/1024/1024


def get_stock_data(ticker, start_date, end_date):
    data = requests.get(f'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={ticker}&type=stock&resolution=D&from={start_date}&to={end_date}').text
    data = data[data.find("["):-1].replace("\"open\"", "\"ticker\":"+ "\""+ ticker +"\""+","+"\"open\"")
    return json.loads(data)

def save2json(data, file_path):
    with open(file_path, 'w') as f:
        json.dump(data, f)

def crawler_job(tickers, start_date, end_date, pbar, idx):
    global save_dir, total_size_crawl
    for ticker in tqdm(tickers, desc='Worker {}'.format(idx), position=idx, leave=False):
        data = get_stock_data(ticker, start_date, end_date)
        data = preprocess(data, ticker)
        save2json(data, f'./{save_dir}/{ticker}.json')
        size = get_size_dir(save_dir)
        pbar.set_postfix_str(f'{size:.2f} MB')
        pbar.update()
        if size > total_size_crawl:
            return 

def preprocess(data, ticker):
    for item in data:
        item["ticker_name"] = ticker
    return data


if __name__ == "__main__":
    save_dir = 'raw_data'
    total_size_crawl = 512
    os.makedirs(save_dir, exist_ok=True)
    tickers = listing_companies()['ticker'].tolist()

    start_date = int(time.mktime(time.strptime("1000-01-01", "%Y-%m-%d")))
    end_date = int(time.mktime(time.strptime("2024-02-01", "%Y-%m-%d")))
    num_thread = os.cpu_count() * 2
    print("Using {} threads to crawl {} tickers".format(num_thread, len(tickers)))

    pbar = tqdm(total=len(tickers), desc='Total Progress')
    processes = []
    for i in range(num_thread):
        p = threading.Thread(target=crawler_job, args=(tickers[i::num_thread], start_date, end_date, pbar, i + 1))
        processes.append(p)

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    pbar.close()





    