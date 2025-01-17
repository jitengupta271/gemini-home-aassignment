All the Queries has written using Postgres syntax.

-- What is the 52 week high and low for all the items traded in the past 3 months?

-- Assumtion: Low and high price has been asked.

with tarded_symbol as (
select 
		distinct symbol 
from 
		fact_symbol_trade 
where 
		trade_execution_date >= current_date - interval '3 months'
)
select 
		max(can.high_price) as high_price,
		min(can.low_price) as low_price
from 
		candle can 
inner join
		tarded_symbol symb
on
		can.symbol=symb.symbol
where 
		can.candle_open_date >= current_date - interval '52 weeks';


-- What is the high/low price/volume in the past 2 hours?

-- Assumtion: I am assuming that data is required by symbol low/high and candle data which has volume column, it provides vollume for the candle time frame.

select 
		symbol,
		min(low_price) as low_price,
		min(volume) as low_volume,
		max(high_price) as high_price,
		max(volume) as high_volume
from
	candle
where candle_open_date >= current_date - interval '2 hours'
group by symbol


-- What is the volume for a given timeframe?

-- Assumtion: I am assuming that it's required by symbol. The candle data which has volume column, it provides vollume for the candle time frame.

select
		symbol,
		sum(volume) as total_volume
from
	 candle
where
	 candle_open_date between <start_date> and <end_date>
group by symbol;

-- Monthly, quarterly and yearly volume for items that have 10 million+ in volume over the past year 
-- (Make assumption that you have also downloaded data for multiple trade pairs).

-- Assumtion: I am assuming that it's sale volume which has been derived from Trade data.

with sysmbol_more_than_10_million as (
select
		symbol,
		sum(symbol_sales_volume)
from
	fact_symbol_trade
where
	extract(year from trade_execution_date) = extract(year from current_date) - 1
group by symbol
having sum(symbol_sales_volume)>10000000
)
select
		fct.symbol,
		'year' as timeframe,
		extract(year from trade_execution_date)	as year,
		-999 as quarter,
		-999 as month,
		sum(symbol_sales_volume) as total_sales_volume
from
	fact_symbol_trade fct
inner join
	sysmbol_more_than_10_million symb
on
	fct.symbol=symb.symbol
group by symbol,timeframe,year,quarter,month
union all
select
		fct.symbol,
		'quarter' as timeframe,
		extract(year from trade_execution_date)	as year,
		extract(quarter from trade_execution_date)	as quarter,
		-999 as month,
		sum(symbol_sales_volume) as total_sales_volume
from
	fact_symbol_trade fct
inner join
	sysmbol_more_than_10_million symb
on
	fct.symbol=symb.symbol
group by symbol,timeframe,year,quarter,month
union all
select
		fct.symbol,
		'month' as timeframe,
		extract(year from trade_execution_date)	as year,
		extract(quarter from trade_execution_date)	as quarter,
		extract(month from trade_execution_date)	as month
from
	fact_symbol_trade fct
inner join
	sysmbol_more_than_10_million symb
on
	fct.symbol=symb.symbol
group by symbol,timeframe,year,quarter,month
