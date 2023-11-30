from apis.treasury.treasury_sdk import Treasury



treas = Treasury()


avg = treas.avg_interest_rates()

print(avg.as_dataframe)