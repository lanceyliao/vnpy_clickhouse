from datetime import datetime
from typing import List

from clickhouse_driver import Client

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
)
from vnpy.trader.setting import SETTINGS


# SQL语句 - K线数据表
BARDATA_CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS dbbardata (
        symbol String,           -- 合约代码
        exchange String,         -- 交易所
        datetime DateTime,       -- 时间
        interval String,         -- 周期
        volume Float64,         -- 成交量
        turnover Float64,       -- 成交额
        open_interest Float64,  -- 持仓量
        open_price Float64,     -- 开盘价
        high_price Float64,     -- 最高价
        low_price Float64,      -- 最低价
        close_price Float64     -- 收盘价
    ) ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(datetime)
    ORDER BY (symbol, exchange, interval, datetime)
"""

# SQL语句 - Tick数据表
TICKDATA_CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS dbtickdata (
        symbol String,           -- 合约代码
        exchange String,         -- 交易所
        datetime DateTime,       -- 时间
        name String,            -- 合约名称
        volume Float64,         -- 成交量
        turnover Float64,       -- 成交额
        open_interest Float64,  -- 持仓量
        last_price Float64,     -- 最新价
        last_volume Float64,    -- 最新成交量
        limit_up Float64,       -- 涨停价
        limit_down Float64,     -- 跌停价
        open_price Float64,     -- 开盘价
        high_price Float64,     -- 最高价
        low_price Float64,      -- 最低价
        pre_close Float64,      -- 昨收价
        bid_price_1 Float64,    -- 买一价
        bid_price_2 Float64,    -- 买二价
        bid_price_3 Float64,    -- 买三价
        bid_price_4 Float64,    -- 买四价
        bid_price_5 Float64,    -- 买五价
        ask_price_1 Float64,    -- 卖一价
        ask_price_2 Float64,    -- 卖二价
        ask_price_3 Float64,    -- 卖三价
        ask_price_4 Float64,    -- 卖四价
        ask_price_5 Float64,    -- 卖五价
        bid_volume_1 Float64,   -- 买一量
        bid_volume_2 Float64,   -- 买二量
        bid_volume_3 Float64,   -- 买三量
        bid_volume_4 Float64,   -- 买四量
        bid_volume_5 Float64,   -- 买五量
        ask_volume_1 Float64,   -- 卖一量
        ask_volume_2 Float64,   -- 卖二量
        ask_volume_3 Float64,   -- 卖三量
        ask_volume_4 Float64,   -- 卖四量
        ask_volume_5 Float64    -- 卖五量
    ) ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(datetime)
    ORDER BY (symbol, exchange, datetime)
"""

# SQL语句 - K线数据汇总表
BAROVERVIEW_CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS dbbaroverview (
        symbol String,           -- 合约代码
        exchange String,         -- 交易所
        interval String,         -- 周期
        count Int64,            -- 数据条数
        start DateTime,         -- 开始时间
        end DateTime            -- 结束时间
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (symbol, exchange, interval)
"""

# SQL语句 - Tick数据汇总表
TICKOVERVIEW_CREATE_SQL = """
    CREATE TABLE IF NOT EXISTS dbtickoverview (
        symbol String,           -- 合约代码
        exchange String,         -- 交易所
        count Int64,            -- 数据条数
        start DateTime,         -- 开始时间
        end DateTime            -- 结束时间
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (symbol, exchange)
"""


class ClickhouseDatabase(BaseDatabase):
    """ClickHouse数据库接口"""

    def __init__(self) -> None:
        """"""
        self._client = Client(
            host=SETTINGS["database.host"],
            user=SETTINGS["database.user"],
            port=SETTINGS["database.port"],
            password=SETTINGS["database.password"],
            database=SETTINGS["database.database"],
        )
        self._client.execute("SELECT 1")

        # 创建数据表
        self._client.execute(BARDATA_CREATE_SQL)
        self._client.execute(TICKDATA_CREATE_SQL)
        self._client.execute(BAROVERVIEW_CREATE_SQL)
        self._client.execute(TICKOVERVIEW_CREATE_SQL)

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存K线数据到数据库"""
        # 准备插入的字段
        fields = [
            "symbol",
            "exchange",
            "datetime",
            "interval",
            "volume",
            "turnover",
            "open_interest",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
        ]

        # 构建插入SQL
        sql = f"""
            INSERT INTO dbbardata (
                {", ".join(fields)}
            ) VALUES
        """

        data = []
        for bar in bars:
            row = [
                bar.symbol,
                bar.exchange.value,  # exchange 是 Exchange 枚举类型
                bar.datetime,
                bar.interval.value,  # interval 是 Interval 枚举类型
                bar.volume,
                bar.turnover,
                bar.open_interest,
                bar.open_price,
                bar.high_price,
                bar.low_price,
                bar.close_price,
            ]
            data.append(row)

        self._client.execute(sql, data)

        # 更新K线汇总数据（增量更新）
        if bars:
            symbol = bars[0].symbol
            exchange = bars[0].exchange.value
            interval = bars[0].interval.value

            # 查询当前汇总值（FINAL 保证读到最新值）
            current = self._client.execute(
                """
                SELECT count, start, end
                FROM dbbaroverview FINAL
                WHERE symbol = %(symbol)s
                  AND exchange = %(exchange)s
                  AND interval = %(interval)s
                """,
                {"symbol": symbol, "exchange": exchange, "interval": interval},
            )
            
            if current:
                # 已存在：增量更新
                old_count, old_start, old_end = current[0]
                new_count = old_count + len(bars)
                new_start = old_start  # start 不变
                new_end = max(old_end, bars[-1].datetime)
            else:
                # 首次插入
                new_count = len(bars)
                new_start = bars[0].datetime
                new_end = bars[-1].datetime
            
            # 插入新的汇总记录（ReplacingMergeTree 会在后台合并）
            self._client.execute(
                "INSERT INTO dbbaroverview (symbol, exchange, interval, count, start, end) VALUES",
                [[symbol, exchange, interval, new_count, new_start, new_end]]
            )

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存Tick数据到数据库"""
        # 准备插入的字段
        fields = [
            "symbol",
            "exchange",
            "datetime",
            "name",
            "volume",
            "turnover",
            "open_interest",
            "last_price",
            "last_volume",
            "limit_up",
            "limit_down",
            "open_price",
            "high_price",
            "low_price",
            "pre_close",
            "bid_price_1",
            "bid_price_2",
            "bid_price_3",
            "bid_price_4",
            "bid_price_5",
            "ask_price_1",
            "ask_price_2",
            "ask_price_3",
            "ask_price_4",
            "ask_price_5",
            "bid_volume_1",
            "bid_volume_2",
            "bid_volume_3",
            "bid_volume_4",
            "bid_volume_5",
            "ask_volume_1",
            "ask_volume_2",
            "ask_volume_3",
            "ask_volume_4",
            "ask_volume_5",
        ]

        # 构建插入SQL
        sql = f"""
            INSERT INTO dbtickdata (
                {", ".join(fields)}
            ) VALUES
        """

        data = []
        for tick in ticks:
            row = [
                tick.symbol,
                tick.exchange.value,
                tick.datetime,
                tick.name,
                tick.volume,
                tick.turnover,
                tick.open_interest,
                tick.last_price,
                tick.last_volume,
                tick.limit_up,
                tick.limit_down,
                tick.open_price,
                tick.high_price,
                tick.low_price,
                tick.pre_close,
                tick.bid_price_1,
                tick.bid_price_2,
                tick.bid_price_3,
                tick.bid_price_4,
                tick.bid_price_5,
                tick.ask_price_1,
                tick.ask_price_2,
                tick.ask_price_3,
                tick.ask_price_4,
                tick.ask_price_5,
                tick.bid_volume_1,
                tick.bid_volume_2,
                tick.bid_volume_3,
                tick.bid_volume_4,
                tick.bid_volume_5,
                tick.ask_volume_1,
                tick.ask_volume_2,
                tick.ask_volume_3,
                tick.ask_volume_4,
                tick.ask_volume_5,
            ]
            data.append(row)

        self._client.execute(sql, data)

        # 更新Tick汇总数据（增量更新）
        if ticks:
            symbol = ticks[0].symbol
            exchange = ticks[0].exchange.value

            # 查询当前汇总值（FINAL 保证读到最新值）
            current = self._client.execute(
                """
                SELECT count, start, end
                FROM dbtickoverview FINAL
                WHERE symbol = %(symbol)s
                  AND exchange = %(exchange)s
                """,
                {"symbol": symbol, "exchange": exchange},
            )
            
            if current:
                # 已存在：增量更新
                old_count, old_start, old_end = current[0]
                new_count = old_count + len(ticks)
                new_start = old_start  # start 不变
                new_end = max(old_end, ticks[-1].datetime)
            else:
                # 首次插入
                new_count = len(ticks)
                new_start = ticks[0].datetime
                new_end = ticks[-1].datetime
            
            # 插入新的汇总记录（ReplacingMergeTree 会在后台合并）
            self._client.execute(
                "INSERT INTO dbtickoverview (symbol, exchange, count, start, end) VALUES",
                [[symbol, exchange, new_count, new_start, new_end]]
            )

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime,
    ) -> List[BarData]:
        """从数据库加载K线数据"""
        sql = """
            SELECT
                symbol, exchange, datetime, interval,
                volume, turnover, open_interest,
                open_price, high_price, low_price, close_price
            FROM dbbardata FINAL
            WHERE symbol = %(symbol)s
                AND exchange = %(exchange)s
                AND interval = %(interval)s
                AND datetime >= %(start)s
                AND datetime <= %(end)s
            ORDER BY datetime
        """

        data = self._client.execute(
            sql,
            {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": interval.value,
                "start": start,
                "end": end,
            },
        )

        bars: list[BarData] = []
        for row in data:
            bar: BarData = BarData(
                symbol=row[0],
                exchange=Exchange(row[1]),
                datetime=row[2],
                interval=Interval(row[3]),
                volume=row[4],
                turnover=row[5],
                open_interest=row[6],
                open_price=row[7],
                high_price=row[8],
                low_price=row[9],
                close_price=row[10],
                gateway_name="DB",
            )
            bars.append(bar)
        return bars

    def load_tick_data(
        self, symbol: str, exchange: Exchange, start: datetime, end: datetime
    ) -> List[TickData]:
        """从数据库加载Tick数据"""
        sql = """
            SELECT
                symbol, exchange, datetime, name,
                volume, turnover, open_interest,
                last_price, last_volume,
                limit_up, limit_down,
                open_price, high_price, low_price, pre_close,
                bid_price_1, bid_price_2, bid_price_3, bid_price_4, bid_price_5,
                ask_price_1, ask_price_2, ask_price_3, ask_price_4, ask_price_5,
                bid_volume_1, bid_volume_2, bid_volume_3, bid_volume_4, bid_volume_5,
                ask_volume_1, ask_volume_2, ask_volume_3, ask_volume_4, ask_volume_5
            FROM dbtickdata FINAL
            WHERE symbol = %(symbol)s
                AND exchange = %(exchange)s
                AND datetime >= %(start)s
                AND datetime <= %(end)s
            ORDER BY datetime
        """

        data = self._client.execute(
            sql,
            {"symbol": symbol, "exchange": exchange.value, "start": start, "end": end},
        )

        ticks: list[TickData] = []
        for row in data:
            tick: TickData = TickData(
                symbol=row[0],
                exchange=Exchange(row[1]),
                datetime=row[2],
                name=row[3],
                volume=row[4],
                turnover=row[5],
                open_interest=row[6],
                last_price=row[7],
                last_volume=row[8],
                limit_up=row[9],
                limit_down=row[10],
                open_price=row[11],
                high_price=row[12],
                low_price=row[13],
                pre_close=row[14],
                bid_price_1=row[15],
                bid_price_2=row[16],
                bid_price_3=row[17],
                bid_price_4=row[18],
                bid_price_5=row[19],
                ask_price_1=row[20],
                ask_price_2=row[21],
                ask_price_3=row[22],
                ask_price_4=row[23],
                ask_price_5=row[24],
                bid_volume_1=row[25],
                bid_volume_2=row[26],
                bid_volume_3=row[27],
                bid_volume_4=row[28],
                bid_volume_5=row[29],
                ask_volume_1=row[30],
                ask_volume_2=row[31],
                ask_volume_3=row[32],
                ask_volume_4=row[33],
                ask_volume_5=row[34],
            )
            ticks.append(tick)
        return ticks

    def delete_bar_data(
        self, symbol: str, exchange: Exchange, interval: Interval
    ) -> int:
        """删除K线数据"""
        # 先查询要删除的记录数
        count_sql = """
            SELECT count()
            FROM dbbardata
            WHERE symbol = %(symbol)s
                AND exchange = %(exchange)s
                AND interval = %(interval)s
        """
        
        count = self._client.execute(
            count_sql,
            {"symbol": symbol, "exchange": exchange.value, "interval": interval.value},
        )[0][0]
        
        # 删除K线数据
        sql = """
            ALTER TABLE dbbardata
            DELETE WHERE symbol = %(symbol)s
                AND exchange = %(exchange)s
                AND interval = %(interval)s
        """

        self._client.execute(
            sql,
            {"symbol": symbol, "exchange": exchange.value, "interval": interval.value},
        )
        
        # 删除K线汇总数据
        overview_sql = """
            ALTER TABLE dbbaroverview
            DELETE WHERE symbol = %(symbol)s
                AND exchange = %(exchange)s
                AND interval = %(interval)s
        """
        
        self._client.execute(
            overview_sql,
            {"symbol": symbol, "exchange": exchange.value, "interval": interval.value},
        )
        
        return count

    def delete_tick_data(self, symbol: str, exchange: Exchange) -> int:
        """删除Tick数据"""
        # 先查询要删除的记录数
        count_sql = """
            SELECT count()
            FROM dbtickdata
            WHERE symbol = %(symbol)s
                AND exchange = %(exchange)s
        """
        
        count = self._client.execute(
            count_sql,
            {"symbol": symbol, "exchange": exchange.value},
        )[0][0]
        
        # 删除Tick数据
        sql = """
            ALTER TABLE dbtickdata
            DELETE WHERE symbol = %(symbol)s
                AND exchange = %(exchange)s
        """

        self._client.execute(sql, {"symbol": symbol, "exchange": exchange.value})
        
        # 删除Tick汇总数据
        overview_sql = """
            ALTER TABLE dbtickoverview
            DELETE WHERE symbol = %(symbol)s
                AND exchange = %(exchange)s
        """
        
        self._client.execute(overview_sql, {"symbol": symbol, "exchange": exchange.value})
        
        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """获取K线数据汇总信息"""
        sql = """
            SELECT 
                symbol, exchange, interval,
                count, start, end
            FROM dbbaroverview FINAL
            ORDER BY symbol, exchange, interval
        """

        data = self._client.execute(sql)
        overviews: list[BarOverview] = []
        for row in data:
            overview = BarOverview(
                symbol=row[0],
                exchange=Exchange(row[1]),
                interval=Interval(row[2]),
                count=row[3],
                start=row[4],
                end=row[5],
            )
            overviews.append(overview)
        return overviews

    def get_tick_overview(self) -> List[TickOverview]:
        """获取Tick数据汇总信息"""
        sql = """
            SELECT 
                symbol, exchange,
                count, start, end
            FROM dbtickoverview FINAL
            ORDER BY symbol, exchange
        """

        data = self._client.execute(sql)
        overviews = []
        for row in data:
            overview = TickOverview(
                symbol=row[0],
                exchange=Exchange(row[1]),
                count=row[2],
                start=row[3],
                end=row[4],
            )
            overviews.append(overview)
        return overviews
