
from fudstop.apis.fed_print.fed_print_sync import FedPrintSync



import asyncio

fp = FedPrintSync()
def main():
    x = fp.search('options')

    print(x.as_dataframe)

main()