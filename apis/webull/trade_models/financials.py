import pandas as pd

class FinancialStatement:
    def __init__(self, r):
        data = r['data']
        self.quote_id = [i.get("quoteId", None) for i in data]
        self.type = [i.get("type", None) for i in data]
        self.fiscal_year = [i.get("fiscalYear", None) for i in data]
        self.fiscal_period = [i.get("fiscalPeriod", None) for i in data]
        self.end_date = [i.get("endDate", None) for i in data]
        self.currency_id = [i.get("currencyId", None) for i in data]
        self.publish_date = [i.get("publishDate", None) for i in data]
        self.total_revenue = [i.get("totalRevenue", None) for i in data]
        self.revenue = [i.get("revenue", None) for i in data]
        self.cost_of_revenue_total = [i.get("costofRevenueTotal", None) for i in data]
        self.gross_profit = [i.get("grossProfit", None) for i in data]
        self.operating_expense = [i.get("operatingExpense", None) for i in data]
        self.sell_gen_admin_expenses = [i.get("sellGenAdminExpenses", None) for i in data]
        self.depreciation_and_amortization = [i.get("depreciationAndAmortization", None) for i in data]
        self.inter_expse_inc_net_oper = [i.get("interExpseIncNetOper", None) for i in data]
        self.unusual_expense_income = [i.get("unusualExpenseIncome", None) for i in data]
        self.operating_income = [i.get("operatingIncome", None) for i in data]
        self.inter_inc_expse_net_non_oper = [i.get("interIncExpseNetNonOper", None) for i in data]
        self.net_income_before_tax = [i.get("netIncomeBeforeTax", None) for i in data]
        self.income_tax = [i.get("incomeTax", None) for i in data]
        self.net_income_after_tax = [i.get("netIncomeAfterTax", None) for i in data]
        self.net_income_before_extra = [i.get("netIncomeBeforeExtra", None) for i in data]
        self.total_extraordinary_items = [i.get("totalExtraordinaryItems", None) for i in data]
        self.net_income = [i.get("netIncome", None) for i in data]
        self.income_avaito_com_excl_extra_ord = [i.get("incomeAvaitoComExclExtraOrd", None) for i in data]
        self.income_avaito_com_incl_extra_ord = [i.get("incomeAvaitoComInclExtraOrd", None) for i in data]
        self.diluted_net_income = [i.get("dilutedNetIncome", None) for i in data]
        self.diluted_weighted_average_shares = [i.get("dilutedWeightedAverageShares", None) for i in data]
        self.diluted_eps_excl_extra_items = [i.get("dilutedEPSExclExtraItems", None) for i in data]
        self.diluted_eps_incl_extra_items = [i.get("dilutedEPSInclExtraItems", None) for i in data]
        self.diluted_normalized_eps = [i.get("dilutedNormalizedEPS", None) for i in data]
        self.operating_profit = [i.get("operatingProfit", None) for i in data]
        self.earning_after_tax = [i.get("earningAfterTax", None) for i in data]
        self.earning_before_tax = [i.get("earningBeforeTax", None) for i in data]
        
        self.data_dict ={
            'quoteId': self.quote_id,
            'type': self.type,
            'fiscalYear': self.fiscal_year,
            'fiscalPeriod': self.fiscal_period,
            'endDate': self.end_date,
            'currencyId': self.currency_id,
            'publishDate': self.publish_date,
            'totalRevenue': self.total_revenue,
            'revenue': self.revenue,
            'costofRevenueTotal': self.cost_of_revenue_total,
            'grossProfit': self.gross_profit,
            'operatingExpense': self.operating_expense,
            'sellGenAdminExpenses': self.sell_gen_admin_expenses,
            'depreciationAndAmortization': self.depreciation_and_amortization,
            'interExpseIncNetOper': self.inter_expse_inc_net_oper,
            'unusualExpenseIncome': self.unusual_expense_income,
            'operatingIncome': self.operating_income,
            'interIncExpseNetNonOper': self.inter_inc_expse_net_non_oper,
            'netIncomeBeforeTax': self.net_income_before_tax,
            'incomeTax': self.income_tax,
            'netIncomeAfterTax': self.net_income_after_tax,
            'netIncomeBeforeExtra': self.net_income_before_extra,
            'totalExtraordinaryItems': self.total_extraordinary_items,
            'netIncome': self.net_income,
            'incomeAvaitoComExclExtraOrd': self.income_avaito_com_excl_extra_ord,
            'incomeAvaitoComInclExtraOrd': self.income_avaito_com_incl_extra_ord,
            'dilutedNetIncome': self.diluted_net_income,
            'dilutedWeightedAverageShares': self.diluted_weighted_average_shares,
            'dilutedEPSExclExtraItems': self.diluted_eps_excl_extra_items,
            'dilutedEPSInclExtraItems': self.diluted_eps_incl_extra_items,
            'dilutedNormalizedEPS': self.diluted_normalized_eps,
            'operatingProfit': self.operating_profit,
            'earningAfterTax': self.earning_after_tax,
            'earningBeforeTax': self.earning_before_tax
        }

        self.df = pd.DataFrame(self.data_dict)
    @classmethod
    def from_dict(cls, data):
        return cls(data)

    def __repr__(self):
        return f'<FinancialStatement quote_id={self.quote_id} fiscal_year={self.fiscal_year} fiscal_period={self.fiscal_period}>'


class CashFlow:
    def __init__(self, r):
        data = r['data']
        self.quoteid = [i.get('quoteId', None) for i in data]
        self.type = [i.get('type', None) for i in data]
        self.fiscal_year = [i.get('fiscalYear', None) for i in data]
        self.fiscal_period = [i.get('fiscalPeriod', None) for i in data]
        self.end_date = [i.get('endDate', None) for i in data]
        self.currency_id = [i.get('currencyId', None) for i in data]
        self.publish_date = [i.get('publishDate', None) for i in data]
        self.cash_from_operating_activities = [i.get('cashfromOperatingActivities', None) for i in data]
        self.net_income = [i.get('netIncome', None) for i in data]
        self.depreciation_and_amortization = [i.get('depreciationAndAmortization', None) for i in data]
        self.deferred_taxes = [i.get('deferredTaxes', None) for i in data]
        self.non_cash_items = [i.get('nonCashItems', None) for i in data]
        self.changes_in_working_capital = [i.get('changesinWorkingCapital', None) for i in data]
        self.cash_from_investing_activities = [i.get('cashfromInvestingActivities', None) for i in data]
        self.capital_expenditures = [i.get('capitalExpenditures', None) for i in data]
        self.other_investing_cashflow_items_total = [i.get('otherInvestingCashFlowItemsTotal', None) for i in data]
        self.cash_from_financing_activities = [i.get('cashfromFinancingActivities', None) for i in data]
        self.financing_cashflow_items = [i.get('financingCashFlowItems', None) for i in data]
        self.total_cash_dividends_paid = [i.get('totalCashDividendsPaid', None) for i in data]
        self.issuance_retirement_of_stock_net = [i.get('issuanceRetirementofStockNet', None) for i in data]
        self.issuance_retirement_of_debt_net = [i.get('issuanceRetirementofDebtNet', None) for i in data]
        self.foreign_exchange_effects = [i.get('foreignExchangeEffects', None) for i in data]
        self.net_change_in_cash = [i.get('netChangeinCash', None) for i in data]

        self.data_dict = {
            'quoteId': self.quoteid,
            'type': self.type,
            'fiscalYear': self.fiscal_year,
            'fiscalPeriod': self.fiscal_period,
            'endDate': self.end_date,
            'currencyId': self.currency_id,
            'publishDate': self.publish_date,
            'cashfromOperatingActivities': self.cash_from_operating_activities,
            'netIncome': self.net_income,
            'depreciationAndAmortization': self.depreciation_and_amortization,
            'deferredTaxes': self.deferred_taxes,
            'nonCashItems': self.non_cash_items,
            'changesinWorkingCapital': self.changes_in_working_capital,
            'cashfromInvestingActivities': self.cash_from_investing_activities,
            'capitalExpenditures': self.capital_expenditures,
            'otherInvestingCashFlowItemsTotal': self.other_investing_cashflow_items_total,
            'cashfromFinancingActivities': self.cash_from_financing_activities,
            'financingCashFlowItems': self.financing_cashflow_items,
            'totalCashDividendsPaid': self.total_cash_dividends_paid,
            'issuanceRetirementofStockNet': self.issuance_retirement_of_stock_net,
            'issuanceRetirementofDebtNet': self.issuance_retirement_of_debt_net,
            'foreignExchangeEffects': self.foreign_exchange_effects,
            'netChangeinCash': self.net_change_in_cash
        }
      
        self.df = pd.DataFrame(self.data_dict)

    @classmethod
    def from_dict(cls, data):
        return cls(data)

    def __repr__(self):
        return f'<CashFlow quote_id={self.quoteid} fiscal_year={self.fiscal_year} fiscal_period={self.fiscal_period}>'



class BalanceSheet:
    def __init__(self, r):
        data = r['data']

        all_data_dicts = []
    
        self.quoteId = [i.get('quoteId', None) for i in data]
        self.type = [i.get('type', None) for i in data]
        self.fiscalYear = [i.get('fiscalYear', None) for i in data]
        self.fiscalPeriod = [i.get('fiscalPeriod', None) for i in data]
        self.endDate = [i.get('endDate', None) for i in data]
        self.currencyId = [i.get('currencyId', None) for i in data]
        self.publishDate = [i.get('publishDate', None) for i in data]
        self.totalAssets = [i.get('totalAssets', None) for i in data]
        self.totalCurrentAssets = [i.get('totalCurrentAssets', None) for i in data]
        self.cashAndShortTermInvest = [i.get('cashAndShortTermInvest', None) for i in data]
        self.cashEquivalents = [i.get('cashEquivalents', None) for i in data]
        self.shortTermInvestments = [i.get('shortTermInvestments', None) for i in data]
        self.totalReceivablesNet = [i.get('totalReceivablesNet', None) for i in data]
        self.accountsReceTradeNet = [i.get('accountsReceTradeNet', None) for i in data]
        self.totalInventory = [i.get('totalInventory', None) for i in data]
        self.prepaidExpenses = [i.get('prepaidExpenses', None) for i in data]
        self.otherCurrentAssetsTotal = [i.get('otherCurrentAssetsTotal', None) for i in data]
        self.totalNonCurrentAssets = [i.get('totalNonCurrentAssets', None) for i in data]
        self.ppeTotalNet = [i.get('ppeTotalNet', None) for i in data]
        self.ppeTotalGross = [i.get('ppeTotalGross', None) for i in data]
        self.accumulatedDepreciationTotal = [i.get('accumulatedDepreciationTotal', None) for i in data]
        self.otherLongTermAssetsTotal = [i.get('otherLongTermAssetsTotal', None) for i in data]
        self.totalLiabilities = [i.get('totalLiabilities', None) for i in data]
        self.totalCurrentLiabilities = [i.get('totalCurrentLiabilities', None) for i in data]
        self.accountsPayable = [i.get('accountsPayable', None) for i in data]
        self.accruedExpenses = [i.get('accruedExpenses', None) for i in data]
        self.notesPayableShortTermDebt = [i.get('notesPayableShortTermDebt', None) for i in data]
        self.currentPortofLTDebtCapitalLeases = [i.get('currentPortofLTDebtCapitalLeases', None) for i in data]
        self.totalNonCurrentLiabilities = [i.get('totalNonCurrentLiabilities', None) for i in data]
        self.totalLongTermDebt = [i.get('totalLongTermDebt', None) for i in data]
        self.longTermDebt = [i.get('longTermDebt', None) for i in data]
        self.totalDebt = [i.get('totalDebt', None) for i in data]
        self.otherLiabilitiesTotal = [i.get('otherLiabilitiesTotal', None) for i in data]
        self.totalEquity = [i.get('totalEquity', None) for i in data]
        self.totalStockhodersEquity = [i.get('totalStockhodersEquity', None) for i in data]
        self.commonStock = [i.get('commonStock', None) for i in data]
        self.additionalPaidInCapital = [i.get('additionalPaidInCapital', None) for i in data]
        self.retainedEarnings = [i.get('retainedEarnings', None) for i in data]
        self.otherEquityTotal = [i.get('otherEquityTotal', None) for i in data]
        self.totalLiabilitiesShareholdersEquity = [i.get('totalLiabilitiesShareholdersEquity', None) for i in data]
        self.totalCommonSharesOutstanding = [i.get('totalCommonSharesOutstanding', None) for i in data]
        self.data_dict = {
            'quoteId': self.quoteId,
            'type': self.type,
            'fiscalYear': self.fiscalYear,
            'fiscalPeriod': self.fiscalPeriod,
            'endDate': self.endDate,
            'currencyId': self.currencyId,
            'publishDate': self.publishDate,
            'totalAssets': self.totalAssets,
            'totalCurrentAssets': self.totalCurrentAssets,
            'cashAndShortTermInvest': self.cashAndShortTermInvest,
            'cashEquivalents': self.cashEquivalents,
            'shortTermInvestments': self.shortTermInvestments,
            'totalReceivablesNet': self.totalReceivablesNet,
            'accountsReceTradeNet': self.accountsReceTradeNet,
            'totalInventory': self.totalInventory,
            'prepaidExpenses': self.prepaidExpenses,
            'otherCurrentAssetsTotal': self.otherCurrentAssetsTotal,
            'totalNonCurrentAssets': self.totalNonCurrentAssets,
            'ppeTotalNet': self.ppeTotalNet,
            'ppeTotalGross': self.ppeTotalGross,
            'accumulatedDepreciationTotal': self.accumulatedDepreciationTotal,
            'otherLongTermAssetsTotal': self.otherLongTermAssetsTotal,
            'totalLiabilities': self.totalLiabilities,
            'totalCurrentLiabilities': self.totalCurrentLiabilities,
            'accountsPayable': self.accountsPayable,
            'accruedExpenses': self.accruedExpenses,
            'notesPayableShortTermDebt': self.notesPayableShortTermDebt,
            'currentPortofLTDebtCapitalLeases': self.currentPortofLTDebtCapitalLeases,
            'totalNonCurrentLiabilities': self.totalNonCurrentLiabilities,
            'totalLongTermDebt': self.totalLongTermDebt,
            'longTermDebt': self.longTermDebt,
            'totalDebt': self.totalDebt,
            'otherLiabilitiesTotal': self.otherLiabilitiesTotal,
            'totalEquity': self.totalEquity,
            'totalStockhodersEquity': self.totalStockhodersEquity,
            'commonStock': self.commonStock,
            'additionalPaidInCapital': self.additionalPaidInCapital,
            'retainedEarnings': self.retainedEarnings,
            'otherEquityTotal': self.otherEquityTotal,
            'totalLiabilitiesShareholdersEquity': self.totalLiabilitiesShareholdersEquity,
            'totalCommonSharesOutstanding': self.totalCommonSharesOutstanding
        }

          
        self.df = pd.DataFrame(self.data_dict)
    @classmethod
    def from_dict(cls, data):
        return cls(data)

class Forecast:
    def __init__(self, data):
        self.id = data.get('id')
        self.title = data.get('title')
        self.currencyName = data.get('currencyName')
        self.points = data.get('points')

    def __str__(self):
        return f"{self.title}: {self.points}"

    def get_trend(self):
        if len(self.points) < 2:
            return "Unknown"
        
        if self.points[-1].get("valueForecast", 0) > self.points[-2].get("valueForecast", 0):
            return "Increasing"
        else:
            return "Decreasing"