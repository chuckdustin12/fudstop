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
        self.total_revenue = [float(i.get("totalRevenue")) if i.get("totalRevenue") is not None else 0 for i in data]
        self.revenue = [float(i.get("revenue")) if i.get("revenue") is not None else 0 for i in data]
        self.cost_of_revenue_total = [float(i.get("costofRevenueTotal")) if i.get("costofRevenueTotal") is not None else 0 for i in data]
        self.gross_profit = [float(i.get("grossProfit")) if i.get("grossProfit") is not None else 0 for i in data]
        self.operating_expense = [float(i.get("operatingExpense")) if i.get("operatingExpense") is not None else 0 for i in data]
        self.sell_gen_admin_expenses = [float(i.get("sellGenAdminExpenses")) if i.get("sellGenAdminExpenses") is not None else 0 for i in data]
        self.depreciation_and_amortization = [float(i.get("depreciationAndAmortization")) if i.get("depreciationAndAmortization") is not None else 0 for i in data]
        self.inter_expse_inc_net_oper = [float(i.get("interExpseIncNetOper")) if i.get("interExpseIncNetOper") is not None else 0 for i in data]
        self.unusual_expense_income = [float(i.get("unusualExpenseIncome")) if i.get("unusualExpenseIncome") is not None else 0 for i in data]
        self.operating_income = [float(i.get("operatingIncome")) if i.get("operatingIncome") is not None else None for i in data]
        self.inter_inc_expse_net_non_oper = [float(i.get("interIncExpseNetNonOper")) if i.get("interIncExpseNetNonOper") is not None else 0 for i in data]
        self.net_income_before_tax = [float(i.get("netIncomeBeforeTax")) if i.get("netIncomeBeforeTax") is not None else 0 for i in data]
        self.income_tax = [float(i.get("incomeTax")) if i.get("incomeTax") is not None else None for i in data]
        self.net_income_after_tax = [float(i.get("netIncomeAfterTax")) if i.get("netIncomeAfterTax") is not None else 0 for i in data]
        self.net_income_before_extra = [float(i.get("netIncomeBeforeExtra")) if i.get("netIncomeBeforeExtra") is not None else 0 for i in data]
        self.total_extraordinary_items = [float(i.get("totalExtraordinaryItems")) if i.get("totalExtraordinaryItems") is not None else 0 for i in data]
        self.net_income = [float(i.get("netIncome")) if i.get("netIncome") is not None else None for i in data]
        self.income_avaito_com_excl_extra_ord = [float(i.get("incomeAvaitoComExclExtraOrd")) if i.get("incomeAvaitoComExclExtraOrd") is not None else 0 for i in data]
        self.income_avaito_com_incl_extra_ord = [float(i.get("incomeAvaitoComInclExtraOrd")) if i.get("incomeAvaitoComInclExtraOrd") is not None else 0 for i in data]
        self.diluted_net_income = [float(i.get("dilutedNetIncome")) if i.get("dilutedNetIncome") is not None else 0 for i in data]
        self.diluted_weighted_average_shares = [float(i.get("dilutedWeightedAverageShares")) if i.get("dilutedWeightedAverageShares") is not None else 0 for i in data]
        self.diluted_eps_excl_extra_items = [float(i.get("dilutedEPSExclExtraItems")) if i.get("dilutedEPSExclExtraItems") is not None else 0 for i in data]
        self.diluted_eps_incl_extra_items = [float(i.get("dilutedEPSInclExtraItems")) if i.get("dilutedEPSInclExtraItems") is not None else 0 for i in data]
        self.diluted_normalized_eps = [float(i.get("dilutedNormalizedEPS")) if i.get("dilutedNormalizedEPS") is not None else 0 for i in data]
        self.operating_profit = [float(i.get("operatingProfit")) if i.get("operatingProfit") is not None else 0 for i in data]
        self.earning_after_tax = [float(i.get("earningAfterTax")) if i.get("earningAfterTax") is not None else 0 for i in data]
        self.earning_before_tax = [float(i.get("earningBeforeTax")) if i.get("earningBeforeTax") is not None else 0 for i in data]

                
        self.data_dict = {
            'quote_id': self.quote_id,
            'type': self.type,
            'fiscal_year': self.fiscal_year,
            'fiscal_period': self.fiscal_period,
            'end_date': self.end_date,
            'currency_id': self.currency_id,
            'publish_date': self.publish_date,
            'total_revenue': self.total_revenue,
            'revenue': self.revenue,
            'cost_of_revenue_total': self.cost_of_revenue_total,
            'gross_profit': self.gross_profit,
            'operating_expense': self.operating_expense,
            'sell_gen_admin_expenses': self.sell_gen_admin_expenses,
            'depreciation_and_amortization': self.depreciation_and_amortization,
            'interest_expense_income_net_operating': self.inter_expse_inc_net_oper,
            'unusual_expense_income': self.unusual_expense_income,
            'operating_income': self.operating_income,
            'interest_income_expense_net_non_operating': self.inter_inc_expse_net_non_oper,
            'net_income_before_tax': self.net_income_before_tax,
            'income_tax': self.income_tax,
            'net_income_after_tax': self.net_income_after_tax,
            'net_income_before_extra': self.net_income_before_extra,
            'total_extraordinary_items': self.total_extraordinary_items,
            'net_income': self.net_income,
            'income_available_to_common_excl_extraordinary': self.income_avaito_com_excl_extra_ord,
            'income_available_to_common_incl_extraordinary': self.income_avaito_com_incl_extra_ord,
            'diluted_net_income': self.diluted_net_income,
            'diluted_weighted_average_shares': self.diluted_weighted_average_shares,
            'diluted_eps_excl_extraordinary_items': self.diluted_eps_excl_extra_items,
            'diluted_eps_incl_extraordinary_items': self.diluted_eps_incl_extra_items,
            'diluted_normalized_eps': self.diluted_normalized_eps,
            'operating_profit': self.operating_profit,
            'earnings_after_tax': self.earning_after_tax,
            'earnings_before_tax': self.earning_before_tax
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
        self.quoteid = [float(i.get('quoteId')) if i.get('quoteId') is not None else 'N/A' for i in data]
        self.type = [float(i.get('type')) if i.get('type') is not None else 'N/A' for i in data]
        self.fiscal_year = [i.get('fiscalYear') if i.get('fiscalYear') is not None else 'N/A' for i in data]
        self.fiscal_period = [i.get('fiscalPeriod') if i.get('fiscalPeriod') is not None else 'N/A' for i in data]
        self.end_date = [i.get('endDate') if i.get('endDate') is not None else 'N/A' for i in data]
        self.currency_id = [float(i.get('currencyId')) if i.get('currencyId') is not None else 0 for i in data]
        self.publish_date = [i.get('publishDate') if i.get('publishDate') is not None else 0 for i in data]
        self.cash_from_operating_activities = [float(i.get('cashfromOperatingActivities')) if i.get('cashfromOperatingActivities') is not None else 0 for i in data]
        self.net_income = [float(i.get('netIncome')) if i.get('netIncome') is not None else 0 for i in data]
        self.depreciation_and_amortization = [float(i.get('depreciationAndAmortization')) if i.get('depreciationAndAmortization') is not None else 0 for i in data]
        self.deferred_taxes = [float(i.get('deferredTaxes')) if i.get('deferredTaxes') is not None else 0 for i in data]
        self.non_cash_items = [float(i.get('nonCashItems')) if i.get('nonCashItems') is not None else 0 for i in data]
        self.changes_in_working_capital = [float(i.get('changesinWorkingCapital')) if i.get('changesinWorkingCapital') is not None else 0 for i in data]
        self.cash_from_investing_activities = [float(i.get('cashfromInvestingActivities')) if i.get('cashfromInvestingActivities') is not None else 0 for i in data]
        self.capital_expenditures = [float(i.get('capitalExpenditures')) if i.get('capitalExpenditures') is not None else 0 for i in data]
        self.other_investing_cashflow_items_total = [float(i.get('otherInvestingCashFlowItemsTotal')) if i.get('otherInvestingCashFlowItemsTotal') is not None else 0 for i in data]
        self.cash_from_financing_activities = [float(i.get('cashfromFinancingActivities')) if i.get('cashfromFinancingActivities') is not None else 0 for i in data]
        self.financing_cashflow_items = [float(i.get('financingCashFlowItems')) if i.get('financingCashFlowItems') is not None else 0 for i in data]
        self.total_cash_dividends_paid = [float(i.get('totalCashDividendsPaid')) if i.get('totalCashDividendsPaid') is not None else 0 for i in data]
        self.issuance_retirement_of_stock_net = [float(i.get('issuanceRetirementofStockNet')) if i.get('issuanceRetirementofStockNet') is not None else None for i in data]
        self.issuance_retirement_of_debt_net = [float(i.get('issuanceRetirementofDebtNet')) if i.get('issuanceRetirementofDebtNet') is not None else 0 for i in data]
        self.foreign_exchange_effects = [float(i.get('foreignExchangeEffects')) if i.get('foreignExchangeEffects') is not None else 0 for i in data]
        self.net_change_in_cash = [float(i.get('netChangeinCash')) if i.get('netChangeinCash') is not None else 0 for i in data]

        self.data_dict = {
            'type': self.type,
            'fiscal_year': self.fiscal_year,
            'fiscal_period': self.fiscal_period,
            'endDate': self.end_date,
            'publishDate': self.publish_date,
            'cash_from_operating_activities': self.cash_from_operating_activities,
            'net_income': self.net_income,
            'depreciationAndAmortization': self.depreciation_and_amortization,
            'deferred_taxes': self.deferred_taxes,
            'non_cash_items': self.non_cash_items,
            'chainges_in_working_capital': self.changes_in_working_capital,
            'cash_from_investing_activities': self.cash_from_investing_activities,
            'capital_expenditures': self.capital_expenditures,
            'other_investing_cash_flow_items_total': self.other_investing_cashflow_items_total,
            'cash_from_financing_activities': self.cash_from_financing_activities,
            'financing_cash_flow_items': self.financing_cashflow_items,
            'total_cash_dividends_paid': self.total_cash_dividends_paid,
            'issuance_retirement_of_stock_net': self.issuance_retirement_of_stock_net,
            'issuance_retirement_of_debt_net': self.issuance_retirement_of_debt_net,
            'foreign_exchange_effects': self.foreign_exchange_effects,
            'net_change_in_cash': self.net_change_in_cash
        }
      
        self.df = pd.DataFrame(self.data_dict)

    @classmethod
    def from_dict(cls, data):
        return cls(data)

    def __repr__(self):
        return f'<CashFlow quote_id={self.quoteid} fiscal_year={self.fiscal_year} fiscal_period={self.fiscal_period}>'



class BalanceSheet:
    def __init__(self, r):
        data = r['data'] if 'data' in r else None

        if data is not None:
            all_data_dicts = []

            self.quoteId = [i.get('quoteId', None) for i in data]
            self.type = [i.get('type', None) for i in data]
            self.fiscalYear = [i.get('fiscalYear', None) for i in data]
            self.fiscalPeriod = [i.get('fiscalPeriod', None) for i in data]
            self.endDate = [i.get('endDate', None) for i in data]
            self.publishDate = [i.get('publishDate', None) for i in data]
            self.currencyId = [i.get('currencyId', None) for i in data]
            
            self.totalAssets = [float(i.get('totalAssets')) if i.get('totalAssets') is not None else 0 for i in data]
            self.totalCurrentAssets = [float(i.get('totalCurrentAssets')) if i.get('totalCurrentAssets') is not None else 0 for i in data]
            self.cashAndShortTermInvest = [float(i.get('cashAndShortTermInvest')) if i.get('cashAndShortTermInvest') is not None else 0 for i in data]
            self.cashEquivalents = [float(i.get('cashEquivalents')) if i.get('cashEquivalents') is not None else None for i in data]
            self.shortTermInvestments = [float(i.get('shortTermInvestments')) if i.get('shortTermInvestments') is not None else 0 for i in data]
            self.totalReceivablesNet = [float(i.get('totalReceivablesNet')) if i.get('totalReceivablesNet') is not None else 0 for i in data]
            self.accountsReceTradeNet = [float(i.get('accountsReceTradeNet')) if i.get('accountsReceTradeNet') is not None else 0 for i in data]
            self.totalInventory = [float(i.get('totalInventory')) if i.get('totalInventory') is not None else 0 for i in data]
            self.prepaidExpenses = [float(i.get('prepaidExpenses')) if i.get('prepaidExpenses') is not None else 0 for i in data]
            self.otherCurrentAssetsTotal = [float(i.get('otherCurrentAssetsTotal')) if i.get('otherCurrentAssetsTotal') is not None else 0 for i in data]
            self.totalNonCurrentAssets = [float(i.get('totalNonCurrentAssets')) if i.get('totalNonCurrentAssets') is not None else 0 for i in data]
            self.ppeTotalNet = [float(i.get('ppeTotalNet')) if i.get('ppeTotalNet') is not None else 0 for i in data]
            self.ppeTotalGross = [float(i.get('ppeTotalGross')) if i.get('ppeTotalGross') is not None else 0 for i in data]
            self.accumulatedDepreciationTotal = [float(i.get('accumulatedDepreciationTotal')) if i.get('accumulatedDepreciationTotal') is not None else 0 for i in data]
            self.otherLongTermAssetsTotal = [float(i.get('otherLongTermAssetsTotal')) if i.get('otherLongTermAssetsTotal') is not None else 0 for i in data]
            self.totalLiabilities = [float(i.get('totalLiabilities')) if i.get('totalLiabilities') is not None else 0 for i in data]
            self.totalCurrentLiabilities = [float(i.get('totalCurrentLiabilities')) if i.get('totalCurrentLiabilities') is not None else 0 for i in data]
            self.accountsPayable = [float(i.get('accountsPayable')) if i.get('accountsPayable') is not None else 0 for i in data]
            self.accruedExpenses = [float(i.get('accruedExpenses')) if i.get('accruedExpenses') is not None else 0 for i in data]
            self.notesPayableShortTermDebt = [float(i.get('notesPayableShortTermDebt')) if i.get('notesPayableShortTermDebt') is not None else 0 for i in data]
            self.currentPortofLTDebtCapitalLeases = [float(i.get('currentPortofLTDebtCapitalLeases')) if i.get('currentPortofLTDebtCapitalLeases') is not None else 0 for i in data]
            self.totalNonCurrentLiabilities = [float(i.get('totalNonCurrentLiabilities')) if i.get('totalNonCurrentLiabilities') is not None else 0 for i in data]
            self.totalLongTermDebt = [float(i.get('totalLongTermDebt')) if i.get('totalLongTermDebt') is not None else 0 for i in data]
            self.longTermDebt = [float(i.get('longTermDebt')) if i.get('longTermDebt') is not None else 0 for i in data]
            self.totalDebt = [float(i.get('totalDebt')) if i.get('totalDebt') is not None else 0 for i in data]
            self.otherLiabilitiesTotal = [float(i.get('otherLiabilitiesTotal')) if i.get('otherLiabilitiesTotal') is not None else 0 for i in data]
            self.totalEquity = [float(i.get('totalEquity')) if i.get('totalEquity') is not None else None for i in data]
            self.totalStockhodersEquity = [float(i.get('totalStockhodersEquity')) if i.get('totalStockhodersEquity') is not None else None for i in data]
            self.commonStock = [float(i.get('commonStock')) if i.get('commonStock') is not None else 0 for i in data]
            self.additionalPaidInCapital = [float(i.get('additionalPaidInCapital')) if i.get('additionalPaidInCapital') is not None else None for i in data]
            self.retainedEarnings = [float(i.get('retainedEarnings')) if i.get('retainedEarnings') is not None else 0 for i in data]
            self.otherEquityTotal = [float(i.get('otherEquityTotal')) if i.get('otherEquityTotal') is not None else 0 for i in data]
            self.totalLiabilitiesShareholdersEquity = [float(i.get('totalLiabilitiesShareholdersEquity')) if i.get('totalLiabilitiesShareholdersEquity') is not None else 0 for i in data]
            self.totalCommonSharesOutstanding = [float(i.get('totalCommonSharesOutstanding')) if i.get('totalCommonSharesOutstanding') is not None else 0 for i in data]

            self.data_dict = {
                'type': self.type,
                'fiscal_year': self.fiscalYear,
                'fiscal_period': self.fiscalPeriod,
                'end_date': self.endDate,
                'publish_date': self.publishDate,
                'total_assets': self.totalAssets,
                'total_current_assets': self.totalCurrentAssets,
                'cash_and_short_term_invest': self.cashAndShortTermInvest,
                'cash_equivalents': self.cashEquivalents,
                'short_term_investments': self.shortTermInvestments,
                'total_receivables_net': self.totalReceivablesNet,
                'accounts_receivable_trade_net': self.accountsReceTradeNet,
                'total_inventory': self.totalInventory,
                'prepaid_expenses': self.prepaidExpenses,
                'other_current_assets_total': self.otherCurrentAssetsTotal,
                'total_non_current_assets': self.totalNonCurrentAssets,
                'ppe_total_net': self.ppeTotalNet,
                'ppe_total_gross': self.ppeTotalGross,
                'accumulated_depreciation_total': self.accumulatedDepreciationTotal,
                'other_long_term_assets_total': self.otherLongTermAssetsTotal,
                'total_liabilities': self.totalLiabilities,
                'total_current_liabilities': self.totalCurrentLiabilities,
                'accounts_payable': self.accountsPayable,
                'accrued_expenses': self.accruedExpenses,
                'notes_payable_short_term_debt': self.notesPayableShortTermDebt,
                'current_port_of_lt_debt_capital_leases': self.currentPortofLTDebtCapitalLeases,
                'total_non_current_liabilities': self.totalNonCurrentLiabilities,
                'total_long_term_debt': self.totalLongTermDebt,
                'long_term_debt': self.longTermDebt,
                'total_debt': self.totalDebt,
                'other_liabilities_total': self.otherLiabilitiesTotal,
                'total_equity': self.totalEquity,
                'total_stockholders_equity': self.totalStockhodersEquity,
                'common_stock': self.commonStock,
                'additional_paid_in_capital': self.additionalPaidInCapital,
                'retained_earnings': self.retainedEarnings,
                'other_equity_total': self.otherEquityTotal,
                'total_liabilities_shareholders_equity': self.totalLiabilitiesShareholdersEquity,
                'total_common_shares_outstanding': self.totalCommonSharesOutstanding
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