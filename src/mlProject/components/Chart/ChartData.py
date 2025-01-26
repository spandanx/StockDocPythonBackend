import requests
import json

class ChartData:
    def __init__(self, chart_base_url):
        self.chart_base_url = chart_base_url

    def enrich_stock_info(self, stock_array):
        headers_ = ["timestamp", "open", "high", "low", "close", "volume"]
        result_array = []
        for row in stock_array:
            result_array.append({headers_[0]: row[0],
                                 headers_[1]: row[1],
                                 headers_[2]: row[2],
                                 headers_[3]: row[3],
                                 headers_[4]: row[4],
                                 headers_[5]: row[5]
                                 })
        return result_array

    def get_chart(self, stock_id, frequency, user_id, oi, from_date, to_date, auth_header):
        url = self.chart_base_url.format(stock_id=stock_id, frequency=frequency, user_id=user_id, oi=oi, from_date=from_date, to_date=to_date)
        # print(url)
        response = requests.get(url, headers=auth_header)
        # print(response)
        json_response = json.loads(response.text)
        enriched_response = self.enrich_stock_info(json_response["data"]["candles"])
        return enriched_response
    #     requests.get(, params={key: value}, args)

if __name__ == "__main__":
    "{1} {ham} {0} {foo} {1}".format(10, 20, foo='bar', ham='spam')
    # https://kite.zerodha.com/oms/instruments/historical/1276417/30minute?user_id=CCN088&oi=1&from=2024-11-25&to=2025-01-24
    str_ = "https://kite.zerodha.com/oms/instruments/historical/{stock_id}/{frequency}?user_id={user_id}&oi={oi}&from={from_date}&to={to_date}"
    abc = str_.format(stock_id="1276417", frequency="30minute", user_id="CCN088", oi="1", from_date="2024-11-25", to_date="2025-01-24")
    print(abc)