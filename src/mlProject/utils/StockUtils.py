import requests
import json

class StockUtils:
    def __init__(self, stock_url_property):
        self.holding_url = stock_url_property["baseurl"] + stock_url_property["holdingurl"]

    def map_holdings(self, response):
        rows = []
        print("Called map_holdings()")
        print(response)
        for row in response:
            # print(row)
            # trading_symbol, qty, last_price, symbol, exchange
            # rows.append({"tradingsymbol": row["exchange"] + "_EQ_" + row["tradingsymbol"],
            #              "qty": row["quantity"] + (row["t1_quantity"] if "t1_quantity" in row else 0),
            #              "last_price": row["last_price"],
            #              "average_price": row["average_price"],
            #              "total_price": round(
            #         abs(row["quantity"] + (row["t1_quantity"] if "t1_quantity" in row else 0)) * row["last_price"], 2),
            #              "symbol": row["tradingsymbol"], "exchange": row["exchange"], "instrument_token": row["instrument_token"]})
            rows.append({
                        "tradingsymbol_with_exchange": row["exchange"] + "_EQ_" + row["tradingsymbol"],
                         "quantity": row["quantity"],
                         "last_price": row["last_price"],
                         "average_price": round(row["average_price"], 2),
                         "current_value": round(row["last_price"]*row["quantity"], 2),
                         "profit_and_loss": round(row["pnl"], 2),
                         "net_change": round(((row["last_price"] - row["average_price"])*row["quantity"])/row["average_price"], 2),
                         "day_change": round(row["day_change_percentage"], 2),
                         "symbol": row["tradingsymbol"],
                         "exchange": row["exchange"],
                         "instrument_token": row["instrument_token"],
                         "total_price": round(
                        abs(row["quantity"] + (row["t1_quantity"] if "t1_quantity" in row else 0)) * row["last_price"], 2)
                         })
        rows.sort(key=lambda x: -x['total_price'])
        return rows

    def get_holdings(self, auth_header):
        print("called get_holdings()")
        print(auth_header)
        url = self.holding_url
        # print(url)
        # print(auth_header)
        response = requests.get(url, headers=auth_header)
        response = json.loads(response.text)
        print(response)
        print("response")
        if 'data' in response:
            return self.map_holdings(response["data"])
            # return response["data"]
        return []
