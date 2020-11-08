# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EasymoneyItem(scrapy.Item):
    # code = update = date = kpicat = kpi = value = source = scrapy.Field()
    code = content = url = totalValue = netValue = NPValue = PE = BE = GP = NP = ROE = DoD = totalValue_rank =\
        netValue_rank = netProfit_rank = PE_rank = BE_rank = GP_rank = NP_rank = ROE_rank = MoneyList \
        = ItemSource = scrapy.Field()
