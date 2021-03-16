import pandas as pd
import requests
import time
import re
from requests import Timeout, HTTPError

def finaltrial():

    # data retrieval
    url = "https://www.nseindia.com/"
    url1 = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
    url2 = "https://www.nseindia.com/api/option-chain-equities?symbol="
    url3 = "https://www.nseindia.com/api/option-chain-indices?symbol="
    headers = {
        'authority': 'www.nseindia.com',
        'method': 'GET',
        'path': '/api/live-analysis-oi-spurts-underlyings',
        'scheme': 'https',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.101 Safari/537.36',
    }
    finaldf = pd.DataFrame()
    regexp = re.compile(r'^[A-Z]')
    count = 0
    errcnt = 0

    # periodic refresh
    while count != 98:
        pcrdf = pd.DataFrame(columns=['pcrOI', 'pcrVol'])
        session = requests.Session()
        try:
            request = session.get(url, headers=headers, timeout=5)
            cookies = dict(request.cookies)
            response = session.get(url1, headers=headers, cookies=cookies, timeout=5)
            response.raise_for_status()
        except Timeout as st:
            errcnt += 1
            print("Timed out. Error: " + str(st) + ". Retrying.\n")
            finaldf.to_csv('C:\\Users\\Gladios\\Desktop\\OIOutputDumpAfterError' + str(errcnt) + '.csv', mode='w',
                              index=False, header=True)
            session.close()
            continue
        except HTTPError as reqex:
            errcnt += 1
            print("HTTP error: " + str(reqex) + ".  Retrying.\n")
            finaldf.to_csv('C:\\Users\\Gladios\\Desktop\\OIOutputDumpAfterError' + str(errcnt) + '.csv', mode='w',
                               index=False, header=True)
            session.close()
            continue
        else:
            kl2 = response.json()["data"]
            klts2 = response.json()["timestamp"]
            print("\nTimestamp - " + str(klts2))
            df1 = pd.DataFrame(kl2, columns=["symbol", "latestOI"])
            df1.loc[len(df1)] = ['#Time stamp', str(klts2)]
            df1.sort_values(["symbol"], axis=0,
                               ascending=True, inplace=True)
            df1.reset_index(drop=True, inplace=True)
            if count != 0:
                df1 = df1[["latestOI"]].copy()
                df1.reset_index(drop=True, inplace=True)

            # attaches OI
            finaldf = pd.concat([finaldf, df1], axis=1)
            finaldf.replace(regex=r'&+', value='%26', inplace=True)

            # pcr
            print("Processing PCR...")
            for row in finaldf.itertuples():
                session = requests.Session()
                if regexp.search(row.symbol):
                    regexp1 = re.compile(r'NIFTY+')
                    try:
                        if regexp1.search(row.symbol):
                            resp1 = session.get(url3 + str(row.symbol), headers=headers, cookies=cookies, timeout=5)
                            resp1.raise_for_status()
                        else:
                            resp1 = session.get(url2 + str(row.symbol), headers=headers, cookies=cookies, timeout=5)
                            resp1.raise_for_status()
                    except Timeout as st:
                        errcnt += 1
                        print("Timed out. Error: " + str(st) + ". Retrying.\n")
                        finaldf.to_csv('C:\\Users\\Gladios\\Desktop\\OIOutputDumpAfterError' + str(errcnt) + '.csv',
                                           mode='w',
                                           index=False, header=True)
                        session.close()
                        continue
                    else:
                        kl3 = resp1.json()["filtered"]["PE"]
                        kl4 = resp1.json()["filtered"]["CE"]
                        pcrOI = kl3["totOI"] / kl4["totOI"]
                        pcrVol = kl3["totVol"] / kl4["totVol"]
                        pcrdf = pcrdf.append({'pcrOI': round(pcrOI, 3), 'pcrVol': round(pcrVol, 3)},
                                                 ignore_index=True)
                        session.close()
                else:
                    pcrdf = pcrdf.append({'pcrOI': 'dum', 'pcrVol': 'dum'}, ignore_index=True)
                    session.close()

            # attaches pcr
            pcrdf.reset_index(drop=True, inplace=True)
            finaldf = pd.concat([finaldf, pcrdf], axis=1)

            del df1, pcrdf

            # hourly dump
            if count % 4 == 0:  # 4 minutes for one count
                finaldf.to_csv('C:\\Users\\Gladios\\Desktop\\OIOutputDump' + str(count) + '.csv', mode='w',
                                   index=False, header=True)
        count = count + 1
        print("No. of extractions " + str(count))
        session.close()
        time.sleep(240)

    # final dump
    finaldf.replace(regex=r'%26', value='&', inplace=True)
    finaldf.to_csv('C:\\Users\\Gladios\\Desktop\\OIOutputDump_final.csv', mode='w',
                   index=False, header=True)
    print("\nCompleted.")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(finaltrial())
